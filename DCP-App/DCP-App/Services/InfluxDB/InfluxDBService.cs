using InfluxDB.Client;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using DCP_App.Services.Mqtt;
using DCP_App.Entities;
using InfluxDB.Client.Linq;
using InfluxDB.Client.Api.Domain;
using DCP_App.InfluxConverters;

namespace DCP_App.Services.InfluxDB
{
    public class InfluxDBService : IInfluxDBService
    {
        private readonly ILogger<MqttConsumerService> _logger;
        private readonly IConfiguration _config;

        private readonly string _token;
        private readonly string _host;
        private readonly string _bucket;
        private readonly string _org;
        private readonly string _measurement;

        private readonly InfluxDBClientOptions _options;
        private readonly SensorEntityConverter _converter;
        private readonly InfluxDBClient _client;

        public InfluxDBService(ILogger<MqttConsumerService> logger, IConfiguration config)
        {
            this._logger = logger;
            this._config = config;

            this._token = config["InfluxDB:Token"]!;
            this._host = config["InfluxDB:Host"]!;
            this._bucket = config["InfluxDB:Bucket"]!;
            this._org = config["InfluxDB:Org"]!;
            this._measurement = config["InfluxDB:Measurement"]!;

            this._options = new InfluxDBClientOptions(this._host)
            {
                Token = this._token,
                Org = this._org,
                Bucket = this._bucket
            };

            this._converter = new SensorEntityConverter();
            this._client = new InfluxDBClient(this._options);
        }

        public async Task WriteAsync(List<SensorEntity> sensorEntities)
        {
            if(!await this._client.PingAsync())
                _logger.LogWarning("InfluxDB - GetLatestByClientId: No DB connection...");

            _logger.LogDebug("InfluxDB - ReadAll: Write");
            await this._client.GetWriteApiAsync(this._converter)
                .WriteMeasurementsAsync(sensorEntities, WritePrecision.S);
            _logger.LogDebug("InfluxDB - ReadAll: After");
        }

        public List<SensorEntity> ReadAll()
        {
            IsConnected();
            _logger.LogDebug("InfluxDB - ReadAll: Before query");
            var queryApi = this._client!.GetQueryApiSync(this._converter);
            //
            // Select ALL
            //
            var query = from s in InfluxDBQueryable<SensorEntity>.Queryable(this._bucket, this._org, queryApi, this._converter)
                        select s;
            List<SensorEntity> result = query.ToList();
            _logger.LogDebug("InfluxDB - ReadAll: After query");
            return result;
        }
        
        public List<SensorEntity> ReadAfterTimestamp(DateTimeOffset timestamp)
        {
            IsConnected();
            _logger.LogDebug("InfluxDB - ReadAfterTimestamp: Before query");
            var queryApi = this._client!.GetQueryApiSync(this._converter);
            //
            // Select ALL
            //
            var query = from s in InfluxDBQueryable<SensorEntity>.Queryable(this._bucket, this._org, queryApi, this._converter)
                        where s.Timestamp > timestamp
                        select s;
            List<SensorEntity> result = query.ToList();
            _logger.LogDebug("InfluxDB - ReadAfterTimestamp: After query");
            return result;
        }        

        public SensorEntity? GetLatestByClientId(string clientId)
        {
            IsConnected();
            _logger.LogDebug("InfluxDB - GetLatestByClientId: Before query");
            var queryApi = this._client!.GetQueryApiSync(this._converter);
            var query = (from s in InfluxDBQueryable<SensorEntity>.Queryable(this._bucket, this._org, queryApi, this._converter)
                        where s.DcpClientId == clientId
                        orderby s.Timestamp descending
                        select s).TakeLast(1);

            SensorEntity? sensorEntity = query.ToList().FirstOrDefault();
            _logger.LogDebug("InfluxDB - GetLatestByClientId: After query");
            return query.ToList().FirstOrDefault();
        }

        public bool IsConnected()
        {
            var connStatus = this._client.PingAsync();
            connStatus.Wait();
            if (!connStatus.Result)
                _logger.LogWarning("InfluxDB - GetLatestByClientId: No DB connection...");
            return connStatus.Result;
        }

        ~InfluxDBService()
        {
            this._client.Dispose();
        }
    }
}
