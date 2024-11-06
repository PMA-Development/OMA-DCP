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
        private readonly string _listenTopic;
        private readonly string _clientId;
        private readonly string _username;
        private readonly string _password;

        private readonly MqttFactory _mqttFactory;
        private readonly IMqttClient _mqttClient;

        private readonly string _appClientId;

        private readonly int _concurrentProcesses;

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
            _listenTopic = $"telemetry/{this._appClientId}";

            _mqttFactory = new MqttFactory();
            _mqttClient = _mqttFactory.CreateMqttClient();
        }

        public async Task StartWorker(CancellationToken shutdownToken)
        {
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

                            _logger.LogInformation($"Received message: {payload}");
                            if (ea.ApplicationMessage.Topic == this._listenTopic && ea.ApplicationMessage.ResponseTopic != string.Empty)
                            {
                                var result = JsonConvert.DeserializeObject<JToken>(payload);
                                if (result != null)
                                {
                                    DateTime? timestamp = null;

                                    if (result.Contains("LastTimestamp"))
                                    {
                                        if (DateTime.TryParse(result["LastTimestamp"]!.ToString(), out _))
                                        {
                                            timestamp = DateTime.Parse(result["LastTimestamp"]!.ToString());
                                        }
                                    }

                                    await PublishSensorData(ea.ApplicationMessage.ResponseTopic, timestamp, shutdownToken);

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
                    .WithTopic(this._listenTopic)
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

        private async Task PublishSensorData(string topic, DateTime? timestamp, CancellationToken shutdownToken)
        {
            List<SensorEntity> sensors;
            if (timestamp == null)
                sensors = this._influxDBService.ReadAll();
            else
                sensors = this._influxDBService.ReadAfterTimestamp((DateTime)timestamp);

            var applicationMessage = new MqttApplicationMessageBuilder()
                .WithTopic(topic)
                .WithPayload(JsonConvert.SerializeObject(sensors))
                .Build();

            await _mqttClient.PublishAsync(applicationMessage, shutdownToken);
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
