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
            _listenTopic = $"dcp/{this._appClientId}";

            _mqttFactory = new MqttFactory();
            _mqttClient = _mqttFactory.CreateMqttClient();
        }

        public async Task StartWorker(CancellationToken shutdownToken)
        {
            while (!shutdownToken.IsCancellationRequested)
            {
                try
                {
                    this._influxDBService.ReadAll().ForEach(it => _logger.LogInformation(it.ToString()));
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "An error occurred during MQTT operation.");
                }

                // Check the connection status every 5 seconds
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
