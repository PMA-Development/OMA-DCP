using DCP_App.Models;
using MQTTnet;

namespace DCP_App.Services
{
    public static class ForwardTopicQueues
    {
        public static Queue<MqttApplicationMessageBuilder> Inbound = new Queue<MqttApplicationMessageBuilder>();
        public static Queue<MqttApplicationMessageBuilder> Outbound = new Queue<MqttApplicationMessageBuilder>();
    }
}
