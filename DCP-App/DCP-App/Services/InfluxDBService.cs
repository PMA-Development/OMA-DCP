using InfluxDB.Client;
using Microsoft.Extensions.Configuration;
using DCP_App.Entities;
using InfluxDB.Client.Linq;
using InfluxDB.Client.Api.Domain;
using DCP_App.InfluxConverters;
using DCP_App.Services.Interfaces;

namespace DCP_App.Services
{
    public class InfluxDBService : IInfluxDBService
    {
        internal Serilog.ILogger _logger => Serilog.Log.ForContext<InfluxDBService>();
        private readonly IConfiguration _config;

        private readonly string _token;
        private readonly string _host;
        private readonly string _bucket;
        private readonly string _org;

        private readonly InfluxDBClientOptions _options;
        private readonly SensorEntityConverter _converter;
        private readonly InfluxDBClient _client;

        private readonly string _measurement = "sensor";

        private readonly int _retensionDays;

        public InfluxDBService(IConfiguration config)
        {
            _config = config;

            _token = _config.GetValue<string>("InfluxDB:Token")!;
            _host = _config.GetValue<string>("InfluxDB:Host")!;
            _bucket = _config.GetValue<string>("InfluxDB:Bucket")!;
            _org = _config.GetValue<string>("InfluxDB:Org")!;

            _options = new InfluxDBClientOptions(_host)
            {
                Token = _token,
                Org = _org,
                Bucket = _bucket
            };

            _converter = new SensorEntityConverter();
            _client = new InfluxDBClient(_options);

            _retensionDays = config.GetValue<int>("InfluxDB:RetensionDays", -300);

            // Make it to a negative number, because we need it to be negative for the start range.
            if (_retensionDays < 0)
                _retensionDays *= -1;
        }

        public async Task WriteAsync(List<SensorEntity> sensorEntities)
        {
            if (!await _client.PingAsync())
                _logger.Warning("InfluxDB - GetLatestByClientId: No DB connection...");

            _logger.Debug("InfluxDB - ReadAll: Write");
            await _client.GetWriteApiAsync(_converter)
                .WriteMeasurementsAsync(sensorEntities, WritePrecision.S);
            _logger.Debug("InfluxDB - ReadAll: After");
        }

        public List<SensorEntity> ReadAll()
        {
            IsConnected();
            _logger.Debug("InfluxDB - ReadAll: Before query");
            var queryApi = _client!.GetQueryApiSync(_converter);
            //
            // Select ALL
            //
            var query = from s in InfluxDBQueryable<SensorEntity>.Queryable(_bucket, _org, queryApi, _converter)
                        select s;
            List<SensorEntity> result = query.ToList();
            _logger.Debug("InfluxDB - ReadAll: After query");
            return result;
        }

        public List<SensorEntity> ReadAfterTimestamp(DateTimeOffset timestamp)
        {
            IsConnected();
            _logger.Debug("InfluxDB - ReadAfterTimestamp: Before query");


            var queryApi = _client!.GetQueryApiSync(_converter); ;

            try
            {
                _logger.Information($"InfluxDB - ReadAfterTimestamp: Read from DatetimeOffset: {timestamp.ToString()}");
                var query = from s in InfluxDBQueryable<SensorEntity>.Queryable(_bucket, _org, queryApi, _converter)
                            where s.Timestamp > timestamp && s.Timestamp < DateTimeOffset.Now // All queries most have a start and stop timestamp, or else the query will fail!
                            select s;

                _logger.Debug("InfluxDB - ReadAfterTimestamp: After query");

                return query.ToList();
            }
            catch (Exception e)
            {
                _logger.Error(e, "InfluxDB - ReadAfterTimestamp: ReadAfterTimestamp");
                throw;
            }
        }

        public async Task<DateTimeOffset> GetLatestTimestampByClientId(string clientId)
        {
            IsConnected();
            _logger.Debug("InfluxDB - GetLatestByClientId: Before query");


            var queryApi = _client!.GetQueryApi();
            var fluxQuery = $"from(bucket: \"{_bucket}\")\n"
                            + $" |> range(start: {_retensionDays})"
                            + $" |> filter(fn: (r) => (r[\"_measurement\"] == \"{_measurement}\"))";

            try
            {
                var tables = await queryApi.QueryAsync(fluxQuery, _org);

                DateTimeOffset timestamp = DateTimeOffset.MinValue;

                if (tables != null)
                {
                    foreach (var fluxRecord in tables.SelectMany(fluxTable => fluxTable.Records))
                        timestamp = fluxRecord.GetTimeInDateTime() != null ? (DateTimeOffset)fluxRecord.GetTimeInDateTime()! : timestamp;
                }

                return timestamp;
            }
            catch (Exception e)
            {
                _logger.Error(e, "InfluxDB - GetLatestByClientId: ");
                throw;
            }
        }

        public bool IsConnected()
        {
            var connStatus = _client.PingAsync();
            connStatus.Wait();
            if (!connStatus.Result)
                _logger.Warning("InfluxDB - IsConnected: No DB connection...");
            return connStatus.Result;
        }

        ~InfluxDBService()
        {
            _client.Dispose();
        }
    }
}
