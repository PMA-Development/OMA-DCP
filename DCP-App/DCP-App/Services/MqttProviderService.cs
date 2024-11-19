using MQTTnet.Client;
using MQTTnet;
using Microsoft.Extensions.Configuration;
using Newtonsoft.Json;
using DCP_App.Entities;
using DCP_App.Models;
using MQTTnet.Packets;
using DCP_App.Services.Interfaces;
using DCP_App.Utils;
using DCP_App.Services.Abstracts;

namespace DCP_App.Services
{
    public class MqttProviderService : MqttAbstractService
    {
        internal override Serilog.ILogger _logger => Serilog.Log.ForContext<MqttProviderService>();
        internal string _mqttRequestTopic;
        internal string _mqttPingTopic = "device/outbound/ping";
        internal string _mqttBeaconTopic = "device/inbound/beacon";

        public MqttProviderService(CancellationTokenSource cts, IConfiguration config, IInfluxDBService InfluxDBService) : base(cts, config, InfluxDBService, "MqttProvider")
        {
            _mqttRequestTopic = $"dcp/client/{_clientId}/telemetry/request";
            _mqttForwardTopics.Add("device/outbound/");
        }

        public override void Run()
        {
            if (_config.GetValue<bool>("MqttProvider:Enabled"))
            {
                _ = Task.Run(async () => await StartWorker(), _cancellationToken);
                _ = Task.Run(async () => await PublishDataAvailable());
                _ = Task.Run(async () => await ProcessForwardMessageQueue());
            }
        }

        internal override void OnConnect()
        {
            base.OnConnect();
            // Do a beacon on connect.
            PublishBeacon();
        }

        private async Task PublishSensorData(string topic, DateTimeOffset timestamp)
        {
            List<SensorEntity> sensors;
            sensors = _influxDBService.ReadAfterTimestamp(timestamp);

            foreach (var sensor in sensors)
            {
                sensor.DcpClientId = _clientId;
            }

            var jsonStr = JsonConvert.SerializeObject(sensors);
            _logger.Information(jsonStr);
            _logger.Information(topic);

            var applicationMessage = new MqttApplicationMessageBuilder()
                .WithTopic(topic)
                .WithPayload(jsonStr)
                .WithQualityOfServiceLevel(MQTTnet.Protocol.MqttQualityOfServiceLevel.AtLeastOnce)
                .Build();

            await _mqttClient.PublishAsync(applicationMessage, _cancellationToken);
        }

        private async Task PublishDataAvailable()
        {
            while (!_cancellationToken.IsCancellationRequested)
            {
                if (await _mqttClient.TryPingAsync())
                {
                    try
                    {
                        TelemetryAvailableModel telemetryAvailableModel = new TelemetryAvailableModel { ClientId = _clientId };
                        var applicationMessage = new MqttApplicationMessageBuilder()
                            .WithTopic(_mqttAvailableTopic)
                            .WithResponseTopic(_mqttRequestTopic)
                            .WithPayload(JsonConvert.SerializeObject(telemetryAvailableModel))
                            .WithQualityOfServiceLevel(MQTTnet.Protocol.MqttQualityOfServiceLevel.AtLeastOnce)
                            .Build();

                        await _mqttClient.PublishAsync(applicationMessage, _cancellationToken);
                        // Broadcast every 5 seconds
                        _logger.Debug($"Provider - Publishing {applicationMessage}");
                    }
                    catch (Exception ex)
                    {
                        _logger.Error(ex, "Provider - PublishDataAvailable: Connection failed.");
                    }
                }
                else
                {
                    _logger.Debug($"Provider - Available: Client not connected!");
                }
                await Task.Delay(TimeSpan.FromSeconds(_publishDataAvailableSeconds), _cancellationToken);
            }

        }

        private async Task ProcessForwardMessageQueue()
        {
            while (!_cancellationToken.IsCancellationRequested)
            {
                if (ForwardTopicQueues.Inbound.Count != 0)
                {
                    try
                    {
                        _logger.Information("Provider - Inbound: Sending outbound message");
                        var msg = ForwardTopicQueues.Inbound.Dequeue().Build();
                        await PulishMessage(msg);
                    }
                    catch (Exception e)
                    {
                        _logger.Information(e, "Error in Inbound: ");
                        throw;
                    }
                }

            }

        }

        private void OnTopicOutboundPing(MqttApplicationMessageReceivedEventArgs ea, string payload)
        {
            var applicationMessage = new MqttApplicationMessageBuilder()
                .WithTopic(ea.ApplicationMessage.Topic)
                .WithPayload(JsonConvert.SerializeObject(payload))
                .WithQualityOfServiceLevel(ea.ApplicationMessage.QualityOfServiceLevel);

            if (ea.ApplicationMessage.ResponseTopic != string.Empty)
            {
                applicationMessage.WithResponseTopic(ea.ApplicationMessage.ResponseTopic);
            }
            ForwardTopicQueues.Inbound.Enqueue(applicationMessage);
        }

        internal override async Task OnTopic(MqttApplicationMessageReceivedEventArgs ea, string payload)
        {
            if (ea.ApplicationMessage.Topic == _mqttRequestTopic)
            {
                await OnTopicRequest(payload, ea);
            }
            else if (_mqttForwardTopics.Any(t => ea.ApplicationMessage.Topic.StartsWith(t)))
            {
                OnTopicForward(payload, ea);
            }
        }

        private async Task OnTopicRequest(string? payload, MqttApplicationMessageReceivedEventArgs ea)
        {
            if (ea.ApplicationMessage.ResponseTopic != string.Empty)
            {
                RequestSensorDataModel? requestSensorDataModel = JsonConvert.DeserializeObject<RequestSensorDataModel>(payload!);
                if (requestSensorDataModel != null)
                {
                    await PublishSensorData(ea.ApplicationMessage.ResponseTopic, requestSensorDataModel.Timestamp);

                }
            }
        }

        private void OnTopicForward(string? payload, MqttApplicationMessageReceivedEventArgs ea)
        {
            _logger.Debug($"Outbound: Queuing {ea.ApplicationMessage.Topic}");
            // Publish Ping inbound, and forward message afterwards.
            if (ea.ApplicationMessage.Topic == _mqttPingTopic)
            {
                PublishBeacon();
            }

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

        private void PublishBeacon()
        {
            DeviceBeaconModel deviceBeaconModel = new DeviceBeaconModel { Id = _clientId, Type = _clientType };

            if (_clientType.ToLower() == "turbine")
                deviceBeaconModel.TurbineId = _clientId;
            else if (_clientType.ToLower() == "island")
                deviceBeaconModel.IslandId = _clientId;

            _logger.Debug($"Publishing Beacon");
            var applicationMessage = new MqttApplicationMessageBuilder()
                .WithTopic(_mqttBeaconTopic)
                .WithPayload(JsonConvert.SerializeObject(deviceBeaconModel))
                .WithQualityOfServiceLevel(MQTTnet.Protocol.MqttQualityOfServiceLevel.AtMostOnce);

            ForwardTopicQueues.Inbound.Enqueue(applicationMessage);
        }

        internal override List<MqttClientSubscribeOptions> GetSubScriptionOptions()
        {
            List<MqttClientSubscribeOptions> subscribeOptions = new List<MqttClientSubscribeOptions>();

            MqttTopicFilter? mqttTopicFilter = new MqttTopicFilterBuilder()
                    .WithTopic(_mqttRequestTopic)
                    .WithAtLeastOnceQoS()
                    .Build();

            MqttClientSubscribeOptions subscribeOption = _mqttFactory.CreateSubscribeOptionsBuilder()
                .WithTopicFilter(mqttTopicFilter)
                .Build();

            subscribeOptions.Add(subscribeOption);

            foreach (string forwardTopic in _mqttForwardTopics)
            {

                MqttTopicFilter? forwardTopicFilter = new MqttTopicFilterBuilder()
                .WithTopic(forwardTopic + "#") // Add multilelvel wildcard, to subscribe to all levels
                .WithAtLeastOnceQoS()
                .Build();

                subscribeOptions.Add(_mqttFactory.CreateSubscribeOptionsBuilder()
                    .WithTopicFilter(forwardTopicFilter)
                    .Build());
            }

            return subscribeOptions;
        }
    }
}
