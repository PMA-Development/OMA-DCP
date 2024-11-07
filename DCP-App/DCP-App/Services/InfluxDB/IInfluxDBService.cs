using DCP_App.Entities;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DCP_App.Services.InfluxDB
{
    public interface IInfluxDBService
    {
        //void Write(TelemetryEntity telemetry);
        public Task WriteAsync(List<SensorEntity> sensorEntities);
        public List<SensorEntity> ReadAll();
        public List<SensorEntity> ReadAfterTimestamp(DateTimeOffset timestamp);
        public SensorEntity? GetLatestByClientId(string clientId);
        //Task<List<Telemetry>> QueryDB(string? minimum, string? maximum, string? deviceId);
        //Task<List<string>> QueryDBDeviceIds();
        //Task<Telemetry> GetLatestTelemetry();
    }
}
