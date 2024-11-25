using DCP_App.Models;
using MQTTnet;
using System.Collections.Concurrent;

namespace DCP_App.Utils
{
    public static class ForwardTopicQueues
    {
        public static BlockingCollection<MqttApplicationMessageBuilder> Inbound = new BlockingCollection<MqttApplicationMessageBuilder>();
        public static BlockingCollection<MqttApplicationMessageBuilder> Outbound = new BlockingCollection<MqttApplicationMessageBuilder>();
    }
}
