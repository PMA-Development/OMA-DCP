using DCP_App.Entities;

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
