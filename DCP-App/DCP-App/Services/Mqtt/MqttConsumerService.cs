using MQTTnet.Client;
using MQTTnet;
using Serilog;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using DCP_App.Services.InfluxDB;
using DCP_App.Entities;
using Newtonsoft.Json.Linq;
using System.Reactive;
using DCP_App.Models;

namespace DCP_App.Services.Mqtt
{
    public class MqttConsumerService
    {
        private readonly ILogger<MqttConsumerService> _logger;
        private readonly IConfiguration _config;
        private readonly IInfluxDBService _influxDBService;

        private readonly string _host;
        private readonly int _port = 1883; // default
        private readonly string _clientId;
        private readonly string _username;
        private readonly string _password;

        private readonly string _turbineId;
        private readonly bool _isTurbine;

        private readonly int _concurrentProcesses;

        private readonly string _appClientId;

        private readonly MqttFactory _mqttFactory;
        private readonly IMqttClient _mqttClient;

        private readonly string _sensorTopic;
        private readonly string _receiveTopic;
        private readonly string _availableTopic;

        public MqttConsumerService(ILogger<MqttConsumerService> logger, IConfiguration config, IInfluxDBService InfluxDBService)
        {
            _logger = logger;
            _config = config;
            _influxDBService = InfluxDBService;

            _host = _config["MqttConsumer:Host"]!;
            if (int.TryParse(_config["MqttConsumer:Port"]!, out _))
            {
                _port = int.Parse(_config["MqttConsumer:Port"]!);
            }
            _clientId = _config["MqttConsumer:ClientId"]!;
            _username = _config["MqttConsumer:Username"]!;
            _password = _config["MqttConsumer:Password"]!;

            if (int.TryParse(_config["MqttConsumer:ConcurrentProcesses"]!, out _))
            {
                _concurrentProcesses = int.Parse(_config["MqttConsumer:ConcurrentProcesses"]!);
            }
            else
            {
                _concurrentProcesses = 1;
            }

            this._turbineId = _config["ClientId"]!;
            this._isTurbine = Convert.ToBoolean(_config["IsTurbine"]!);

            this._appClientId = _config["ClientId"]!;

            _mqttFactory = new MqttFactory();
            _mqttClient = _mqttFactory.CreateMqttClient();

            this._sensorTopic = "telemetry/sensor";
            this._receiveTopic = $"telemetry/client/{this._appClientId}/request";
            this._availableTopic = "telemetry/available";
        }

        public async Task StartWorker(CancellationToken shutdownToken)
        {
            /*
             * This sample subscribes to a topic.
             */

            //var concurrent = new SemaphoreSlim(Environment.ProcessorCount);
            var concurrent = new SemaphoreSlim(this._concurrentProcesses);

            try
            {
                var mqttClientOptions = new MqttClientOptionsBuilder()
                    .WithTcpServer(_host, _port) // MQTT broker address and port
                    .WithCredentials(_username, _password) // Set username and password
                    .WithClientId(_clientId)
                    .WithProtocolVersion(MQTTnet.Formatter.MqttProtocolVersion.V500)
                    //.WithCleanSession()
                    .Build();

                _mqttClient.ApplicationMessageReceivedAsync += async ea =>
                {
                    await concurrent.WaitAsync(shutdownToken).ConfigureAwait(false);

                    async Task ProcessAsync()
                    {
                        try
                        {
                            var payload = Encoding.UTF8.GetString(ea.ApplicationMessage.PayloadSegment);

                            _logger.LogDebug($"Consumer - Received message: {payload}");
                            _logger.LogDebug($"Consumer - topic: {ea.ApplicationMessage.Topic}");
                            _logger.LogDebug($"Consumer - ResponseTopic: {ea.ApplicationMessage.ResponseTopic}");
                            if (ea.ApplicationMessage.Topic == this._sensorTopic)
                            {
                                SensorEntity? sensorEntity = JsonConvert.DeserializeObject<SensorEntity>(payload);

                                if (sensorEntity != null)
                                {
                                    if (!sensorEntity.Timestamp.HasValue)
                                    {
                                        sensorEntity.Timestamp = DateTime.UtcNow;
                                    }
                                    if (this._isTurbine)
                                    {
                                        sensorEntity.TurbineId = this._turbineId;
                                    }
                                    sensorEntity.DcpClientId = this._appClientId;

                                    await _influxDBService.WriteAsync(new List<SensorEntity> { sensorEntity });
                                }
                                else
                                {
                                    _logger.LogDebug($"Consumer - Recied no sensor data on topic: {ea.ApplicationMessage.Topic}!");
                                }
                            }

                            else if (ea.ApplicationMessage.Topic == this._availableTopic)
                            {
                                _logger.LogDebug($"Consumer - Processing topic: {ea.ApplicationMessage.Topic}");
                                PublishAvailableModel? publishAvailableModel = JsonConvert.DeserializeObject<PublishAvailableModel>(payload);
                                if (publishAvailableModel != null)
                                {
                                    RequestSensorData(ea.ApplicationMessage.ResponseTopic, publishAvailableModel.ClientId, shutdownToken).Wait();
                                }
                                else
                                {
                                    _logger.LogDebug($"Consumer - No client Id recied on topic {ea.ApplicationMessage.Topic}!");
                                }
                            }

                            else if (ea.ApplicationMessage.Topic == this._receiveTopic)
                            {
                                List<SensorEntity>? sensorEntities = JsonConvert.DeserializeObject<List<SensorEntity>>(payload);
                                if (sensorEntities != null)
                                {
                                    await _influxDBService.WriteAsync(sensorEntities);
                                }
                                else
                                {
                                    _logger.LogDebug($"Consumer - Recieved no sensor data on topic {ea.ApplicationMessage.Topic}!");
                                }
                            }
                        }
                        finally
                        {
                            concurrent.Release();
                        }
                    }

                    _ = Task.Run(ProcessAsync, shutdownToken);
                };

                var mqttSensorTopicFilter = new MqttTopicFilterBuilder()
                    .WithTopic(this._sensorTopic)
                    .WithAtLeastOnceQoS()
                    .Build();

                var sensorSubscribeOption = _mqttFactory.CreateSubscribeOptionsBuilder()
                    .WithTopicFilter(mqttSensorTopicFilter)
                    .Build();

                var mqttReceiveTopicFilter = new MqttTopicFilterBuilder()
                    .WithTopic(this._sensorTopic)
                    .WithAtLeastOnceQoS()
                    .Build();

                var receiveSubscribeOption = _mqttFactory.CreateSubscribeOptionsBuilder()
                    .WithTopicFilter(mqttReceiveTopicFilter)
                    .Build();

                var mqttAvailableTopicFilter = new MqttTopicFilterBuilder()
                    .WithTopic(this._availableTopic)
                    .WithAtLeastOnceQoS()
                    .Build();

                var availableSubscribeOption = _mqttFactory.CreateSubscribeOptionsBuilder()
                    .WithTopicFilter(mqttAvailableTopicFilter)
                    .Build();

                // Handle reconnection logic and cancellation token properly
                while (!shutdownToken.IsCancellationRequested)
                {
                    try
                    {
                        // Periodically check if the connection is alive, otherwise reconnect
                        if (!await _mqttClient.TryPingAsync())
                        {
                            _logger.LogInformation("Consumer - Attempting to connect to MQTT Broker...");
                            await _mqttClient.ConnectAsync(mqttClientOptions, shutdownToken);

                            // Subscribe every time we connect, but keep using the same OptionsBuilder, to avoid subscribing more than once.
                            await _mqttClient.SubscribeAsync(sensorSubscribeOption, shutdownToken);
                            _logger.LogInformation($"Consumer - MQTT client subscribed to {this._sensorTopic}.");

                            // Subscribe every time we connect, but keep using the same OptionsBuilder, to avoid subscribing more than once.
                            await _mqttClient.SubscribeAsync(receiveSubscribeOption, shutdownToken);
                            _logger.LogInformation($"Consumer - MQTT client subscribed to {this._receiveTopic}.");


                            // Subscribe every time we connect, but keep using the same OptionsBuilder, to avoid subscribing more than once.
                            await _mqttClient.SubscribeAsync(availableSubscribeOption, shutdownToken);
                            _logger.LogInformation($"Consumer - MQTT client subscribed to {this._availableTopic}.");
                        }
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Consumer - An error occurred during MQTT operation.");
                    }

                    // Check the connection status every 5 seconds
                    await Task.Delay(TimeSpan.FromSeconds(5), shutdownToken);
                }

                _logger.LogInformation("Consumer - Cancellation requested. Exiting...");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Consumer - Connection failed.");
            }
            finally
            {
                // Dispose of the MQTT client manually at the end
                if (_mqttClient.IsConnected)
                {
                    await _mqttClient.DisconnectAsync();
                }

                _mqttClient.Dispose();
            }
        }

        private async Task RequestSensorData(string topic, string clientId, CancellationToken shutdownToken)
        {
            SensorEntity? sensor = this._influxDBService.GetLatestByClientId(clientId);
            RequestSensorDataModel requestSensorData = new RequestSensorDataModel();
            if ( sensor != null)
            {
                requestSensorData.Timestamp = sensor.Timestamp != null ? (DateTimeOffset)sensor.Timestamp: new DateTimeOffset();
            }

            var applicationMessage = new MqttApplicationMessageBuilder()
                .WithTopic(topic)
                .WithPayload(JsonConvert.SerializeObject(requestSensorData))
                .WithQualityOfServiceLevel(MQTTnet.Protocol.MqttQualityOfServiceLevel.AtLeastOnce)
                .WithResponseTopic(this._receiveTopic)
                .Build();

            await _mqttClient.PublishAsync(applicationMessage, shutdownToken);
        }

        ~MqttConsumerService()
        {
            // Dispose of the MQTT client manually at the end
            if (_mqttClient.IsConnected)
            {
                _mqttClient.DisconnectAsync().Wait();
            }

            _mqttClient.Dispose();
        }
    }
}
