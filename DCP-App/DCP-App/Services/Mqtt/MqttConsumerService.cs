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
using DCP_App.Measurements;

namespace DCP_App.Services.Mqtt
{
    public class MqttConsumerService
    {
        private readonly ILogger<MqttConsumerService> _logger;
        private readonly IConfiguration _config;
        private readonly IInfluxDBService _influxDBService;

        private readonly string _host;
        private readonly int _port = 1883; // default
        private readonly string _topic;
        private readonly string _telemetryTopic;
        private readonly string _clientId;
        private readonly string _username;
        private readonly string _password;

        private readonly string _turbineId;
        private readonly bool _isTurbine;

        private readonly int _concurrentProcesses;

        private readonly MqttFactory _mqttFactory;
        private readonly IMqttClient _mqttClient;

        private readonly string _topicPrefix = "telemetry/";
        private readonly string _topicGenerator = "generator";
        private readonly string _topicPosition = "position";
        private readonly string _topicWeather = "weather";

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
            _topic = _config["MqttConsumer:Topic"]!;
            _telemetryTopic = _config["MqttConsumer:TelemetryTopic"]!;

            if (int.TryParse(_config["MqttConsumer:ConcurrentProcesses"]!, out _))
            {
                _concurrentProcesses = int.Parse(_config["MqttConsumer:ConcurrentProcesses"]!);
            }
            else
            {
                _concurrentProcesses = 1;
            }

            this._turbineId = _config["turbineId"]!;
            this._isTurbine = Convert.ToBoolean(_config["IsTurbine"]!);

            _mqttFactory = new MqttFactory();
            _mqttClient = _mqttFactory.CreateMqttClient();
        }

        public async Task StartWorker(CancellationToken shutdownToken)
        {
            /*
             * This sample subscribes to a topic.
             */

            //var concurrent = new SemaphoreSlim(Environment.ProcessorCount);
            var concurrent = new SemaphoreSlim(2);

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
                            // DO YOUR WORK HERE!
                            await _ProcessRecievedMessage(ea);
                        }
                        finally
                        {
                            concurrent.Release();
                        }
                    }

                    _ = Task.Run(ProcessAsync, shutdownToken);
                };

                var mqttTopicFilter = new MqttTopicFilterBuilder()
                    .WithTopic("telemetry/sensor")
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

        private async Task _ProcessRecievedMessage(MqttApplicationMessageReceivedEventArgs e)
        {
            var payload = Encoding.UTF8.GetString(e.ApplicationMessage.PayloadSegment);

            _logger.LogInformation($"Received message: {payload}");
            if (e.ApplicationMessage.Topic == "telemetry/sensor")
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

                    await _influxDBService.WriteAsync(new List<SensorEntity> { sensorEntity });
                }
                else
                {
                    _logger.LogInformation("Weather measurement was null!");
                }
            }
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
