using MQTTnet.Client;
using MQTTnet;
using System.Text;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using DCP_App.Services.InfluxDB;
using DCP_App.Entities;
using MQTTnet.Server;
using DCP_App.Models;
using System.Security.Authentication;
using System.Security.Cryptography.X509Certificates;

namespace DCP_App.Services.Mqtt
{
    public class MqttProviderService
    {
        private readonly ILogger<MqttProviderService> _logger;
        private readonly IConfiguration _config;
        private readonly IInfluxDBService _influxDBService;

        private readonly string _host;
        private readonly int _port = 1883; // default
        private readonly string _requestTopic;
        private readonly string _clientId;
        private readonly string _username;
        private readonly string _password;

        private readonly MqttFactory _mqttFactory;
        private readonly IMqttClient _mqttClient;

        private readonly string _appClientId;

        private readonly int _concurrentProcesses;

        private readonly string _availableTopic;

        private readonly List<string> _forwardTopics;

        private bool _publishBeacon = true;

        private readonly int _publishDataAvailableSeconds;

        private readonly bool _useTLS;

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

            if (int.TryParse(_config["MqttProvider:ConcurrentProcesses"]!, out _))
            {
                _concurrentProcesses = int.Parse(_config["MqttProvider:ConcurrentProcesses"]!);
            }
            else
            {
                _concurrentProcesses = 1;
            }

            this._appClientId = _config["ClientId"]!;
            _requestTopic = $"dcp/client/{this._appClientId}/telemetry/request";

            _mqttFactory = new MqttFactory();
            _mqttClient = _mqttFactory.CreateMqttClient();

            this._availableTopic = "dcp/telemetry/available";

            this._forwardTopics = new List<string> { "device/outbound/" };

            if (int.TryParse(_config["MqttProvider:PublishDataAvailableSeconds"]!, out _))
            {
                this._publishDataAvailableSeconds = int.Parse(_config["MqttProvider:PublishDataAvailableSeconds"]!);
            }
            else
            {
                this._publishDataAvailableSeconds = 5;
            }

            this._useTLS = !Convert.ToBoolean(this._config["MqttProvider:UseTLS"]);
        }

        public async Task StartWorker(CancellationToken shutdownToken)
        {
            if (!Convert.ToBoolean(this._config["MqttProvider:Enabled"]))
            {
                return;
            }

            // Let the publish run concurrently
            _ = Task.Run(() => PublishDataAvailable(shutdownToken));

            // Process the queue async
            _ = Task.Run(() => ProcessForwardMessageQueue(shutdownToken));

            var concurrent = new SemaphoreSlim(this._concurrentProcesses);

            try
            {
                var mqttClientOptionsBuilder = new MqttClientOptionsBuilder()
                    .WithTcpServer(_host, _port) // MQTT broker address and port
                    .WithCredentials(_username, _password) // Set username and password
                    .WithClientId(_clientId)
                    .WithProtocolVersion(MQTTnet.Formatter.MqttProtocolVersion.V500);

                if (this._useTLS)
                {
                    mqttClientOptionsBuilder.WithTlsOptions(
                    o => o.WithCertificateValidationHandler(
                        // The used public broker sometimes has invalid certificates. This sample accepts all
                        // certificates. This should not be used in live environments.
                        _ => true));
                }

                var mqttClientOptions = mqttClientOptionsBuilder.Build();

                _mqttClient.ApplicationMessageReceivedAsync += async ea =>
                {
                    await concurrent.WaitAsync(shutdownToken).ConfigureAwait(false);

                    async Task ProcessAsync()
                    {
                        try
                        {
                            var payload = Encoding.UTF8.GetString(ea.ApplicationMessage.PayloadSegment);

                            _logger.LogDebug($"Provider - Received message: {payload}");
                            _logger.LogDebug($"Provider - topic: {ea.ApplicationMessage.Topic}");
                            _logger.LogDebug($"Provider - ResponseTopic: {ea.ApplicationMessage.ResponseTopic}");
                            if (ea.ApplicationMessage.Topic == this._requestTopic && ea.ApplicationMessage.ResponseTopic != string.Empty)
                            {
                                RequestSensorDataModel? requestSensorDataModel = JsonConvert.DeserializeObject<RequestSensorDataModel>(payload);
                                if (requestSensorDataModel != null)
                                {
                                    await PublishSensorData(ea.ApplicationMessage.ResponseTopic, requestSensorDataModel.Timestamp, shutdownToken);

                                }
                            }
                            else if (_forwardTopics.Any(t => ea.ApplicationMessage.Topic.StartsWith(t)))
                            { 
                                _logger.LogDebug($"Provider - Outbound: Queuing {ea.ApplicationMessage.Topic}");
                                var applicationMessage = new MqttApplicationMessageBuilder()
                                    .WithTopic(ea.ApplicationMessage.Topic)
                                    .WithPayload(payload)
                                    .WithQualityOfServiceLevel(ea.ApplicationMessage.QualityOfServiceLevel);

                                if (ea.ApplicationMessage.ResponseTopic != string.Empty)
                                {
                                    applicationMessage.WithResponseTopic(ea.ApplicationMessage.ResponseTopic);
                                }
                                ForwardTopicQueues.Outbound.Enqueue(applicationMessage);
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

                List<MqttClientSubscribeOptions> forwardTopicsSubscribeOptions = new List<MqttClientSubscribeOptions>();

                foreach (var forwardTopic in this._forwardTopics)
                {

                    var forwardTopicFilter = new MqttTopicFilterBuilder()
                    .WithTopic(forwardTopic + "#") // Add multilelvel wildcard, to subscribe to all levels
                    .WithAtLeastOnceQoS()
                    .Build();

                    forwardTopicsSubscribeOptions.Add(_mqttFactory.CreateSubscribeOptionsBuilder()
                        .WithTopicFilter(forwardTopicFilter)
                        .Build());
                }

                // Handle reconnection logic and cancellation token properly
                while (!shutdownToken.IsCancellationRequested)
                {
                    try
                    {
                        // Periodically check if the connection is alive, otherwise reconnect
                        if (!await _mqttClient.TryPingAsync())
                        {
                            _logger.LogInformation("Provider - Attempting to connect to MQTT Broker...");
                            await _mqttClient.ConnectAsync(mqttClientOptions, shutdownToken);

                            // Subscribe every time we connect, but keep using the same OptionsBuilder, to avoid subscribing more than once.
                            await _mqttClient.SubscribeAsync(subscribeOption, shutdownToken);
                            _logger.LogInformation($"Provider - MQTT client subscribed to {this._requestTopic}.");

                            foreach (var forwardSubscribeOption in forwardTopicsSubscribeOptions)
                            {
                                // Subscribe every time we connect, but keep using the same OptionsBuilder, to avoid subscribing more than once.
                                await _mqttClient.SubscribeAsync(forwardSubscribeOption, shutdownToken);
                                _logger.LogInformation($"Provider - MQTT client subscribed to forwading topic: {forwardSubscribeOption}.");
                            }

                            if (this._publishBeacon)
                            {
                                
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Provider - An error occurred during MQTT operation.");
                    }

                    // Check the connection status every 5 seconds
                    await Task.Delay(TimeSpan.FromSeconds(5), shutdownToken);
                }

                _logger.LogInformation("Provider - Cancellation requested. Exiting...");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Provider - Connection failed.");
            }
        }

        private async Task PublishSensorData(string topic, DateTimeOffset timestamp, CancellationToken shutdownToken)
        {
            List<SensorEntity> sensors;
            sensors = this._influxDBService.ReadAfterTimestamp(timestamp);

            foreach (var sensor in sensors)
            {
                sensor.DcpClientId = this._appClientId;
            }

            var jsonStr = JsonConvert.SerializeObject(sensors);
            _logger.LogInformation(jsonStr);
            _logger.LogInformation(topic);

            var applicationMessage = new MqttApplicationMessageBuilder()
                .WithTopic(topic)
                .WithPayload(jsonStr)
                .WithQualityOfServiceLevel(MQTTnet.Protocol.MqttQualityOfServiceLevel.AtLeastOnce)
                .Build();

            await _mqttClient.PublishAsync(applicationMessage, shutdownToken);
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
                        _logger.LogDebug($"Provider - Publishing {applicationMessage}");
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Provider - PublishDataAvailable: Connection failed.");
                    }
                }
                else
                {
                    _logger.LogDebug($"Provider - Available: Client not connected!");
                }
                await Task.Delay(TimeSpan.FromSeconds(this._publishDataAvailableSeconds), shutdownToken);
            }
            
        }

        private async Task ProcessForwardMessageQueue(CancellationToken shutdownToken)
        {
            while (!shutdownToken.IsCancellationRequested)
            {
                if (ForwardTopicQueues.Inbound.Count != 0)
                {
                    var msg = ForwardTopicQueues.Inbound.Dequeue().Build();
                    await PulishMessage(msg, shutdownToken);
                }
            }

        }

        public async Task PulishMessage(MqttApplicationMessage? msg, CancellationToken shutdownToken)
        {
            if (await _mqttClient.TryPingAsync())
            {
                try
                {
                    await _mqttClient.PublishAsync(msg, shutdownToken);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Provider - PulbishMessage: Connection failed.");
                }
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
