using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reactive;
using System.Text;
using System.Threading.Tasks;

namespace DCP_App.Entities
{
    public class SensorEntity
    {
        public required string Id { get; set; }
        public required string? TurbineId { get; set; }
        public required string Type { get; set; }
        public DateTimeOffset? Timestamp { get; set; }
        public virtual required ICollection<SensorAttributeEntity> Attributes { get; set; }

        public override string ToString()
        {
            return $"{Timestamp:MM/dd/yyyy hh:mm:ss.fff tt} {Id} type: {Type}, " +
                   $"attributes: {string.Join(", ", Attributes)}.";
        }
    }

    public class SensorAttributeEntity
    {
        public required string Name { get; set; }
        public required string Value { get; set; }
        public override string ToString()
        {
            return $"{Name}={Value}";
        }
    }
}
