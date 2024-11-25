using MQTTnet.Client;
using MQTTnet;
using System.Text;
using Microsoft.Extensions.Configuration;
using Newtonsoft.Json;
using DCP_App.Entities;
using DCP_App.Models;
using DCP_App.Services.Interfaces;
using DCP_App.Utils;
using DCP_App.Services.Abstracts;
using MQTTnet.Packets;

namespace DCP_App.Services
{
    public class MqttConsumerService : MqttAbstractService
    {
        internal override Serilog.ILogger _logger => Serilog.Log.ForContext<MqttConsumerService>();
        // Topic related
        private readonly string _mqttSensorTopic;
        private readonly string _mqttReceiveTopic;

        private readonly bool _providerEnabled;

        public MqttConsumerService(CancellationTokenSource cts, IConfiguration config, IInfluxDBService InfluxDBService) : base(cts, config, InfluxDBService, "MqttConsumer")
        {
            _mqttSensorTopic = "telemetry";
            _mqttReceiveTopic = $"dcp/client/{_clientId}/telemetry/receive";
            _mqttForwardTopics.Add("device/inbound/");

            _providerEnabled = _config.GetValue<bool>("MqttProvider:Enabled");
        }

        public override void Run()
        {

            _ = Task.Run(() => StartWorker());
            _ = Task.Run(() => ProcessForwardMessageQueue());
        }

        #region On Topic
        internal override async Task OnTopic(MqttApplicationMessageReceivedEventArgs ea, string payload)
        {
            if (ea.ApplicationMessage.Topic == _mqttSensorTopic)
                await OnTopicSensor(ea, payload);

            else if (ea.ApplicationMessage.Topic == _mqttAvailableTopic)
                await OnTopicTelemetryAvailable(ea, payload);

            else if (ea.ApplicationMessage.Topic == _mqttReceiveTopic)
                await OnTopicTelemetryReceive(ea, payload);

            else if (_mqttForwardTopics.Any(t => ea.ApplicationMessage.Topic == "device/inbound/beacon") && _providerEnabled)
                OnTopicInboundBeacon(ea, payload);

            else if (_mqttForwardTopics.Any(t => ea.ApplicationMessage.Topic.StartsWith(t)) && _providerEnabled)
                OnTopicInboundForward(ea, payload);

            else
                _logger.Debug($"Unknown topic {ea.ApplicationMessage.Topic}!");
        }

        private async Task OnTopicSensor(MqttApplicationMessageReceivedEventArgs ea, string payload)
        {
            SensorEntity? sensorEntity = JsonConvert.DeserializeObject<SensorEntity>(payload);

            if (sensorEntity != null)
            {
                if (!sensorEntity.Timestamp.HasValue)
                {
                    sensorEntity.Timestamp = DateTime.UtcNow;
                }
                sensorEntity.TurbineId = _clientId;
                sensorEntity.DcpClientId = _clientId;

                await _influxDBService.WriteAsync(new List<SensorEntity> { sensorEntity });
            }
            else
            {
                _logger.Debug($"Received no sensor data on topic: {ea.ApplicationMessage.Topic}!");
            }
        }

        private async Task OnTopicTelemetryAvailable(MqttApplicationMessageReceivedEventArgs ea, string payload)
        {
            _logger.Debug($"Processing topic: {ea.ApplicationMessage.Topic}");
            TelemetryAvailableModel? telemetryAvailableModel = JsonConvert.DeserializeObject<TelemetryAvailableModel>(payload);
            if (telemetryAvailableModel != null)
            {
                await PublishRequestSensorData(ea.ApplicationMessage.ResponseTopic, telemetryAvailableModel.ClientId);
            }
            else
            {
                _logger.Debug($"No client Id recied on topic {ea.ApplicationMessage.Topic}!");
            }
        }

        private async Task OnTopicTelemetryReceive(MqttApplicationMessageReceivedEventArgs ea, string payload)
        {
            List<SensorEntity>? sensorEntities = JsonConvert.DeserializeObject<List<SensorEntity>>(payload);
            if (sensorEntities != null)
                await _influxDBService.WriteAsync(sensorEntities);
            else
                _logger.Debug($"Recieved no sensor data on topic {ea.ApplicationMessage.Topic}!");
        }

        private void OnTopicInboundBeacon(MqttApplicationMessageReceivedEventArgs ea, string payload)
        {
            DeviceBeaconModel? deviceBeaconModel = JsonConvert.DeserializeObject<DeviceBeaconModel>(payload);
            if (deviceBeaconModel == null)
            {
                _logger.Debug($"Inbound Beacon reveived but unable to intercept");
                return;
            }

            _logger.Debug($"Inbound Beacon reveived and intercepted");

            if (_clientType.ToLower() == "turbine")
                deviceBeaconModel.TurbineId = _clientId;
            else if (_clientType.ToLower() == "island")
                deviceBeaconModel.IslandId = _clientId;

            var applicationMessage = new MqttApplicationMessageBuilder()
                .WithTopic(ea.ApplicationMessage.Topic)
                .WithPayload(JsonConvert.SerializeObject(deviceBeaconModel))
                .WithQualityOfServiceLevel(MQTTnet.Protocol.MqttQualityOfServiceLevel.AtMostOnce);

            ForwardTopicQueues.Inbound.Add(applicationMessage);
        }

        private void OnTopicInboundForward(MqttApplicationMessageReceivedEventArgs ea, string payload)
        {
            _logger.Debug($"Inbound: Queuing {ea.ApplicationMessage.Topic}");
            var applicationMessage = new MqttApplicationMessageBuilder()
                .WithTopic(ea.ApplicationMessage.Topic)
                .WithPayload(payload)
                .WithQualityOfServiceLevel(ea.ApplicationMessage.QualityOfServiceLevel);

            if (ea.ApplicationMessage.ResponseTopic != string.Empty)
            {
                applicationMessage.WithResponseTopic(ea.ApplicationMessage.ResponseTopic);
            }
            ForwardTopicQueues.Inbound.Add(applicationMessage);
        }
        #endregion

        private async Task PublishRequestSensorData(string topic, string clientId)
        {
            DateTimeOffset timestamp = await _influxDBService.GetLatestTimestampByClientId(clientId);
            RequestSensorDataModel requestSensorData = new RequestSensorDataModel();
            requestSensorData.Timestamp = timestamp;

            var applicationMessage = new MqttApplicationMessageBuilder()
                .WithTopic(topic)
                .WithPayload(JsonConvert.SerializeObject(requestSensorData))
                .WithQualityOfServiceLevel(MQTTnet.Protocol.MqttQualityOfServiceLevel.AtLeastOnce)
                .WithResponseTopic(_mqttReceiveTopic)
                .Build();

            await _mqttClient.PublishAsync(applicationMessage);
        }

        internal override List<MqttClientSubscribeOptions> GetSubScriptionOptions()
        {
            List<MqttClientSubscribeOptions> subscribeOptions = new List<MqttClientSubscribeOptions>();

            var mqttSensorTopicFilter = new MqttTopicFilterBuilder()
                .WithTopic(_mqttSensorTopic)
                .WithAtLeastOnceQoS()
                .Build();

            var sensorSubscribeOption = _mqttFactory.CreateSubscribeOptionsBuilder()
                .WithTopicFilter(mqttSensorTopicFilter)
                .Build();

            subscribeOptions.Add(sensorSubscribeOption);

            var mqttReceiveTopicFilter = new MqttTopicFilterBuilder()
                .WithTopic(_mqttReceiveTopic)
                .WithAtLeastOnceQoS()
                .Build();

            var receiveSubscribeOption = _mqttFactory.CreateSubscribeOptionsBuilder()
                .WithTopicFilter(mqttReceiveTopicFilter)
                .Build();

            subscribeOptions.Add(receiveSubscribeOption);

            var mqttAvailableTopicFilter = new MqttTopicFilterBuilder()
                .WithTopic(_mqttAvailableTopic)
                .WithAtLeastOnceQoS()
                .Build();

            var availableSubscribeOption = _mqttFactory.CreateSubscribeOptionsBuilder()
                .WithTopicFilter(mqttAvailableTopicFilter)
                .Build();

            subscribeOptions.Add(availableSubscribeOption);

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

        private async Task ProcessForwardMessageQueue()
        {
            try
            { 
                while (!_cancellationToken.IsCancellationRequested)
                {
                    try
                    {
                        _logger.Information("Consumer - Outbound: Sending outbound message");
                        MqttApplicationMessageBuilder msg = ForwardTopicQueues.Outbound.Take(_cancellationToken);
                        await PulishMessage(msg!.Build());
                    }
                    catch (OperationCanceledException e)
                    {
                        _logger.Information(e, "Consumer - Error in Outbound: ");
                        throw;
                    }
                }
            }
            catch (Exception e)
            {
                _logger.Error(e, "ProcessForwardMessageQueue: ");
                _cts.Cancel(); // Close the program.
            }
}
    }
}