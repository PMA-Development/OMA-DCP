using DCP_App.Entities;
using InfluxDB.Client;
using InfluxDB.Client.Api.Domain;
using InfluxDB.Client.Core.Flux.Domain;
using InfluxDB.Client.Linq;
using InfluxDB.Client.Writes;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace DCP_App.InfluxConverters
{
    public class SensorEntityConverter : IDomainObjectMapper, IMemberNameResolver
    {
        public T ConvertToEntity<T>(FluxRecord fluxRecord)
        {
            if (typeof(T) != typeof(SensorEntity))
            {
                throw new NotSupportedException($"This converter doesn't supports: {typeof(SensorEntity)}");
            }

            //
            // Create SensorEntity entity and parse `SeriesId`, `Value` and `Time`
            //
            var customEntity = new SensorEntity
            {
                Id = Convert.ToString(fluxRecord.GetValueByKey("id"))!,
                TurbineId = Convert.ToString(fluxRecord.GetValueByKey("turbine_id"))!,
                Type = Convert.ToString(fluxRecord.GetValueByKey("type"))!,
                Timestamp = fluxRecord.GetTime().GetValueOrDefault().ToDateTimeUtc(),
                Attributes = new List<SensorAttributeEntity>()
            };

            foreach (var (key, value) in fluxRecord.Values)
            {
                //
                // Parse SubCollection values
                //
                if (key.StartsWith("property_"))
                {
                    var attribute = new SensorAttributeEntity
                    {
                        Name = key.Replace("property_", string.Empty),
                        Value = Convert.ToString(value) ?? string.Empty,
                    };

                    customEntity.Attributes.Add(attribute);
                }
            }

            return (T)Convert.ChangeType(customEntity, typeof(T));
        }

        public object ConvertToEntity(FluxRecord fluxRecord, Type type)
        {
            if (type != typeof(SensorEntity))
            {
                throw new NotSupportedException($"This converter doesn't supports: {typeof(SensorEntity)}");
            }

            var customEntity = new SensorEntity
            {
                Id = Convert.ToString(fluxRecord.GetValueByKey("id"))!,
                TurbineId = Convert.ToString(fluxRecord.GetValueByKey("turbine_id"))!,
                Type = Convert.ToString(fluxRecord.GetValueByKey("type"))!,
                Timestamp = fluxRecord.GetTime().GetValueOrDefault().ToDateTimeUtc(),
                Attributes = new List<SensorAttributeEntity>()
            };

            foreach (var (key, value) in fluxRecord.Values)
                if (key.StartsWith("property_"))
                {
                    var attribute = new SensorAttributeEntity
                    {
                        Name = key.Replace("property_", string.Empty),
                        Value = Convert.ToString(value) ?? string.Empty
                    };

                    customEntity.Attributes.Add(attribute);
                }

            return Convert.ChangeType(customEntity, type);
        }

        public PointData ConvertToPointData<T>(T entity, WritePrecision precision)
        {
            if (!(entity is SensorEntity ce))
            {
                throw new NotSupportedException($"This converter doesn't supports: {typeof(SensorEntity)}");
            }

            //
            // Map `SeriesId`, `Value` and `Time` to Tag, Field and Timestamp
            //
            var point = PointData
                .Measurement("sensor")
                .Tag("id", ce.Id.ToString())
                .Field("type", ce.Type)
                .Timestamp((DateTimeOffset)ce.Timestamp!, precision);

            //
            // Map subattributes to Fields
            //
            foreach (var attribute in ce.Attributes ?? new List<SensorAttributeEntity>())
            {
                point = point.Field($"property_{attribute.Name}", attribute.Value);
            }

            return point;
        }

        /// <summary>
        /// How your property is named in InfluxDB.
        /// </summary>
        public string GetColumnName(MemberInfo memberInfo)
        {
            switch (memberInfo.Name)
            {
                case "id":
                    return "id";
                case "Type":
                    return "type";
                case "TurbineId":
                    return "turbine_id";
                default:
                    return memberInfo.Name;
            }
        }

        /// <summary>
        /// Return name for flattened properties.
        /// </summary>
        public string GetNamedFieldName(MemberInfo memberInfo, object value)
        {
            return $"property_{Convert.ToString(value)}";
        }

        /// <summary>
        /// How the Domain Object property is mapped into InfluxDB schema. Is it Timestamp, Tag, ...?
        /// </summary>
        public MemberType ResolveMemberType(MemberInfo memberInfo)
        {
            switch (memberInfo.Name)
            {
                case "Timestamp":
                    return MemberType.Timestamp;
                case "Name":
                    return MemberType.NamedField;
                case "Type":
                    return MemberType.NamedFieldValue;
                case "TurbineId":
                    return MemberType.NamedFieldValue;
                case "Id":
                    return MemberType.Tag;
                default:
                    return MemberType.Field;
            }
        }
    }
}
