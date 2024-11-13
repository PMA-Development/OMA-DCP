using DCP_App.Services.Interfaces;
using Microsoft.Extensions.Configuration;
using MQTTnet.Client;
using MQTTnet;
using System.Text;

namespace DCP_App.Services.Abstracts
{
    public abstract class MqttAbstractService
    {

        internal virtual Serilog.ILogger _logger => Serilog.Log.ForContext<MqttAbstractService>();
        internal readonly IConfiguration _config;
        internal readonly IInfluxDBService _influxDBService;
        internal readonly CancellationToken _cancellationToken;

        // Application settings
        internal readonly string _clientId;
        internal readonly string _clientType;
        internal readonly SemaphoreSlim _concurrentProcesses;
        internal readonly int _publishDataAvailableSeconds;

        // MQTT
        internal readonly MqttFactory _mqttFactory;
        internal readonly IMqttClient _mqttClient;
        internal readonly string _mqttHost;
        internal readonly int _mqttPort;
        internal readonly string _mqttClientId;
        internal readonly string _mqttUsername;
        internal readonly string _mqttPassword;
        internal readonly bool _MqttUseTLS;
        internal readonly string _mqttAvailableTopic = "dcp/telemetry/available";
        internal readonly List<string> _mqttForwardTopics = new List<string>();

        public MqttAbstractService(CancellationTokenSource cts, IConfiguration config, IInfluxDBService InfluxDBService, string serviceType)
        {
            _cancellationToken = cts.Token;
            _config = config;
            _influxDBService = InfluxDBService;

            // Application settings
            _clientId = _config.GetValue<string>("ClientId")!;
            _clientType = _config.GetValue<string>("ClientType")!;
            _concurrentProcesses = new SemaphoreSlim(_config.GetValue(serviceType + ":ConcurrentProcesses", 1));
            _publishDataAvailableSeconds = _config.GetValue(serviceType + ":PublishDataAvailableSeconds", 5);

            // MQTT
            _mqttFactory = new MqttFactory();
            _mqttClient = _mqttFactory.CreateMqttClient();
            _mqttHost = _config.GetValue<string>(serviceType + ":Host")!;
            _mqttPort = _config.GetValue(serviceType + ":Port", 1883);
            _mqttClientId = _config.GetValue<string>(serviceType + ":ClientId")!;
            _mqttUsername = _config.GetValue<string>(serviceType + ":Username")!;
            _mqttPassword = _config.GetValue<string>(serviceType + ":Password")!;
            _MqttUseTLS = _config.GetValue<bool>(serviceType + ":UseTLS");
            // Topic related
            _mqttAvailableTopic = "dcp/telemetry/available";
        }

        internal async Task StartWorker()
        {
            try
            {
                _mqttClient.ApplicationMessageReceivedAsync += async ea =>
                {
                    // Wait for an available process, before creating a new Task.
                    await _concurrentProcesses.WaitAsync(_cancellationToken).ConfigureAwait(false);
                    try
                    {
                        _ = Task.Run(async () => await ProcessApplicationMessageReceivedAsync(ea), _cancellationToken);
                    }
                    finally
                    {
                        _concurrentProcesses.Release();
                    }
                };

                await HandleMqttConnection();

                _logger.Information("Cancellation requested. Exiting...");
            }
            catch (Exception ex)
            {
                _logger.Error(ex, "Connection failed.");
            }
        }

        private async Task ProcessApplicationMessageReceivedAsync(MqttApplicationMessageReceivedEventArgs ea)
        {
            string? payload = Encoding.UTF8.GetString(ea.ApplicationMessage.PayloadSegment);
            _logger.Debug($"Received message \"{payload}\", topic \"{ea.ApplicationMessage.Topic}\", ResponseTopic: \"{ea.ApplicationMessage.ResponseTopic}\"");

            await OnTopic(ea, payload);
        }

        internal abstract Task OnTopic(MqttApplicationMessageReceivedEventArgs ea, string payload);

        internal abstract List<MqttClientSubscribeOptions> GetSubScriptionOptions();

        private async Task HandleMqttConnection()
        {
            MqttClientOptions mqttClientOptions = GetMqttClientOptionsBuilder().Build();
            List<MqttClientSubscribeOptions> subscribeOptions = GetSubScriptionOptions();

            // Handle reconnection logic and cancellation token properly
            while (!_cancellationToken.IsCancellationRequested)
            {
                try
                {
                    // Periodically check if the connection is alive, otherwise reconnect
                    if (!await _mqttClient.TryPingAsync())
                    {
                        _logger.Information("Attempting to connect to MQTT Broker...");
                        await _mqttClient.ConnectAsync(mqttClientOptions, _cancellationToken);

                        foreach (var subscribeOption in subscribeOptions)
                        {
                            await _mqttClient.SubscribeAsync(subscribeOption, _cancellationToken);
                            _logger.Information($"MQTT client subscribed to topic: {subscribeOption}.");
                        }

                        // Method to override.
                        OnConnect();
                    }
                }
                catch (Exception ex)
                {
                    _logger.Error(ex, "An error occurred during MQTT operation.");
                }

                // Check the connection status every 5 seconds
                await Task.Delay(TimeSpan.FromSeconds(5), _cancellationToken);
            }
        }

        internal virtual void OnConnect()
        {

        }

        public async Task PulishMessage(MqttApplicationMessage? msg)
        {
            if (await _mqttClient.TryPingAsync())
            {
                try
                {
                    await _mqttClient.PublishAsync(msg, _cancellationToken);
                }
                catch (Exception ex)
                {
                    _logger.Error(ex, "PulbishMessage: Connection failed.");
                }
            }
        }

        private MqttClientOptionsBuilder GetMqttClientOptionsBuilder()
        {
            MqttClientOptionsBuilder mqttClientOptionsBuilder = new MqttClientOptionsBuilder()
                    .WithTcpServer(_mqttHost, _mqttPort) // MQTT broker address and port
                    .WithCredentials(_mqttUsername, _mqttPassword) // Set username and password
                    .WithClientId(_mqttClientId)
                    .WithProtocolVersion(MQTTnet.Formatter.MqttProtocolVersion.V500);

            if (_MqttUseTLS)
            {
                mqttClientOptionsBuilder.WithTlsOptions(
                o => o.WithCertificateValidationHandler(
                    // The used public broker sometimes has invalid certificates. This sample accepts all
                    // certificates. This should not be used in live environments.
                    _ => true));
            }

            return mqttClientOptionsBuilder;
        }

        public virtual void Run()
        {
            throw new NotImplementedException();
        }

        ~MqttAbstractService()
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
