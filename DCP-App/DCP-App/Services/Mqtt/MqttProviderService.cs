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
using MQTTnet.Server;
using Newtonsoft.Json.Linq;
using DCP_App.Models;

namespace DCP_App.Services.Mqtt
{
    public class MqttProviderService
    {
        private readonly ILogger<MqttProviderService> _logger;
        private readonly IConfiguration _config;
        private readonly IInfluxDBService _influxDBService;

        private readonly string _host;
        private readonly int _port = 1883; // default
        private readonly string _topic;
        private readonly string _requestTopic;
        private readonly string _clientId;
        private readonly string _username;
        private readonly string _password;

        private readonly MqttFactory _mqttFactory;
        private readonly IMqttClient _mqttClient;

        private readonly string _appClientId;

        private readonly int _concurrentProcesses;

        private readonly string _availableTopic;

        public MqttProviderService(ILogger<MqttProviderService> logger, IConfiguration config, IInfluxDBService InfluxDBService)
        {
            _logger = logger;
            _config = config;
            _influxDBService = InfluxDBService;

            _host = _config["MqttProvider:Host"]!;
            if (int.TryParse(_config["MqttProvider:Port"]!, out _))
            {
                _port = int.Parse(_config["MqttProvider:Port"]!);
            }
            _clientId = _config["MqttProvider:ClientId"]!;
            _username = _config["MqttProvider:Username"]!;
            _password = _config["MqttProvider:Password"]!;
            _topic = _config["MqttProvider:Topic"]!;

            if (int.TryParse(_config["MqttProvider:ConcurrentProcesses"]!, out _))
            {
                _concurrentProcesses = int.Parse(_config["MqttProvider:ConcurrentProcesses"]!);
            }
            else
            {
                _concurrentProcesses = 1;
            }

            this._appClientId = _config["ClientId"]!;
            _requestTopic = $"telemetry/client/{this._appClientId}/request";

            _mqttFactory = new MqttFactory();
            _mqttClient = _mqttFactory.CreateMqttClient();

            this._availableTopic = "telemetry/available";
        }

        public async Task StartWorker(CancellationToken shutdownToken)
        {
            if (!Convert.ToBoolean(this._config["MqttProvider:Enabled"]))
            {
                return;
            }

            // Let the publish run concurrently
            _ = PublishDataAvailable(shutdownToken);

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

                            _logger.LogInformation($"Provider - Received message: {payload}");
                            _logger.LogInformation($"Provider - topic: {ea.ApplicationMessage.Topic}");
                            _logger.LogInformation($"Provider - ResponseTopic: {ea.ApplicationMessage.ResponseTopic}");
                            if (ea.ApplicationMessage.Topic == this._requestTopic && ea.ApplicationMessage.ResponseTopic != string.Empty)
                            {
                                RequestSensorDataModel? requestSensorDataModel = JsonConvert.DeserializeObject<RequestSensorDataModel>(payload);
                                if (requestSensorDataModel != null)
                                {
                                    _logger.LogInformation($"Provider - before: {ea.ApplicationMessage.ResponseTopic}");
                                    await PublishSensorData(ea.ApplicationMessage.ResponseTopic, requestSensorDataModel.Timestamp, shutdownToken);

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

                var mqttTopicFilter = new MqttTopicFilterBuilder()
                    .WithTopic(this._requestTopic)
                    .WithAtLeastOnceQoS()
                    .Build();

                var subscribeOption = _mqttFactory.CreateSubscribeOptionsBuilder()
                    .WithTopicFilter(mqttTopicFilter)
                    .Build();

                // Handle reconnection logic and cancellation token properly
                while (!shutdownToken.IsCancellationRequested)
                {
                    try
                    {
                        // Periodically check if the connection is alive, otherwise reconnect
                        if (!await _mqttClient.TryPingAsync())
                        {
                            _logger.LogInformation("Attempting to connect to MQTT Broker...");
                            await _mqttClient.ConnectAsync(mqttClientOptions, shutdownToken);

                            // Subscribe every time we connect, but keep using the same OptionsBuilder, to avoid subscribing more than once.
                            await _mqttClient.SubscribeAsync(subscribeOption, shutdownToken);
                            _logger.LogInformation($"MQTT client subscribed to {_topic}.");
                        }
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "An error occurred during MQTT operation.");
                    }

                    // Check the connection status every 5 seconds
                    await Task.Delay(TimeSpan.FromSeconds(5), shutdownToken);
                }

                _logger.LogInformation("Cancellation requested. Exiting...");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Connection failed.");
            }
        }

        private async Task PublishSensorData(string topic, DateTimeOffset timestamp, CancellationToken shutdownToken)
        {
            _logger.LogInformation($"Provider - 1");
            List<SensorEntity> sensors;
            sensors = this._influxDBService.ReadAfterTimestamp(timestamp);
            _logger.LogInformation($"Provider - 2");

            foreach (var sensor in sensors)
            {
                sensor.DcpClientId = this._appClientId;
            }

            var applicationMessage = new MqttApplicationMessageBuilder()
                .WithTopic(topic)
                .WithPayload(JsonConvert.SerializeObject(sensors))
                .WithQualityOfServiceLevel(MQTTnet.Protocol.MqttQualityOfServiceLevel.AtLeastOnce)
                .Build();

            await _mqttClient.PublishAsync(applicationMessage, shutdownToken);

            _logger.LogInformation($"Provider - 3");
        }

        private async Task PublishDataAvailable(CancellationToken shutdownToken)
        {
            while (!shutdownToken.IsCancellationRequested)
            {
                if (await _mqttClient.TryPingAsync())
                {
                    try
                    {
                        PublishAvailableModel publishAvailableModel = new PublishAvailableModel { ClientId = this._appClientId };
                        var applicationMessage = new MqttApplicationMessageBuilder()
                            .WithTopic(this._availableTopic)
                            .WithResponseTopic(this._requestTopic)
                            .WithPayload(JsonConvert.SerializeObject(publishAvailableModel))
                            .WithQualityOfServiceLevel(MQTTnet.Protocol.MqttQualityOfServiceLevel.AtLeastOnce)
                            .Build();

                        await _mqttClient.PublishAsync(applicationMessage, shutdownToken);
                        // Broadcast every 5 seconds
                        _logger.LogInformation($"Publishing {applicationMessage}");
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Connection failed.");
                    }
                }
                else
                {
                    _logger.LogInformation($"Available: Client not connected!");
                }
                await Task.Delay(TimeSpan.FromSeconds(5), shutdownToken);
            }
            
        }

        ~MqttProviderService()
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
