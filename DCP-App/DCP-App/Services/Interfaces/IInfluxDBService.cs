using DCP_App.Entities;

namespace DCP_App.Services.Interfaces
{
    public interface IInfluxDBService
    {
        public Task WriteAsync(List<SensorEntity> sensorEntities);
        public List<SensorEntity> ReadAll();
        public List<SensorEntity> ReadAfterTimestamp(DateTimeOffset timestamp);
        public Task<DateTimeOffset> GetLatestTimestampByClientId(string clientId);
    }
}
