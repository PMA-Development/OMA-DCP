using DCP_App.Entities;
using DCP_App.InfluxConverters;
using DCP_App.Services.Mqtt;
using InfluxDB.Client;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace DCP_App.Services.InfluxDB
{
    public interface IInfluxDBService
    {
        public Task WriteAsync(List<SensorEntity> sensorEntities);
        public List<SensorEntity> ReadAll();
        public List<SensorEntity> ReadAfterTimestamp(DateTimeOffset timestamp);
        public SensorEntity? GetLatestByClientId(string clientId);
    }
}
