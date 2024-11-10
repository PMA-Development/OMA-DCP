using InfluxDB.Client;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using DCP_App.Services.Mqtt;
using DCP_App.Entities;
using InfluxDB.Client.Linq;
using InfluxDB.Client.Api.Domain;
using DCP_App.InfluxConverters;
using System.Net.Sockets;
using InfluxDB.Client.Flux;
using System;

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

        private readonly InfluxDBClientOptions _options;
        private readonly SensorEntityConverter _converter;
        private readonly InfluxDBClient _client;

        private readonly string _measurement = "sensor";

        private readonly int _retensionDays;

        public InfluxDBService(ILogger<MqttConsumerService> logger, IConfiguration config)
        {
            this._logger = logger;
            this._config = config;

            this._token = config["InfluxDB:Token"]!;
            this._host = config["InfluxDB:Host"]!;
            this._bucket = config["InfluxDB:Bucket"]!;
            this._org = config["InfluxDB:Org"]!;

            this._options = new InfluxDBClientOptions(this._host)
            {
                Token = this._token,
                Org = this._org,
                Bucket = this._bucket
            };

            this._converter = new SensorEntityConverter();
            this._client = new InfluxDBClient(this._options);

            if (int.TryParse(_config["InfluxDB:RetensionDays"]!, out _))
                this._retensionDays = int.Parse(_config["InfluxDB:RetensionDays"]!);
            else 
                this._retensionDays = -300; // defualt to -300

            // Make it to a negative number, because we need it to be negative for the start range.
            if (this._retensionDays < 0)
                this._retensionDays *= -1;
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


            var queryApi = this._client!.GetQueryApiSync(this._converter); ;

            try
            {
                _logger.LogInformation($"InfluxDB - ReadAfterTimestamp: Read from DatetimeOffset: {timestamp.ToString()}");
                var query = from s in InfluxDBQueryable<SensorEntity>.Queryable(this._bucket, this._org, queryApi, this._converter)
                            where s.Timestamp > timestamp && s.Timestamp < DateTimeOffset.Now // All queries most have a start and stop timestamp, or else the query will fail!
                            select s;

                _logger.LogDebug("InfluxDB - ReadAfterTimestamp: After query");

                return query.ToList();
            }
            catch (Exception e)
            {
                _logger.LogError(e, "InfluxDB - ReadAfterTimestamp: ReadAfterTimestamp");
                throw;
            }
        }        

        public async Task<DateTimeOffset> GetLatestByClientId(string clientId)
        {
            IsConnected();
            _logger.LogDebug("InfluxDB - GetLatestByClientId: Before query");


            var queryApi = this._client!.GetQueryApi();
            var fluxQuery = $"from(bucket: \"{this._bucket}\")\n"
                            + $" |> range(start: {this._retensionDays})"
                            + $" |> filter(fn: (r) => (r[\"_measurement\"] == \"{this._measurement}\"))";

            try
            {
                var tables = await queryApi.QueryAsync(fluxQuery, this._org);

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
                _logger.LogError(e, "InfluxDB - GetLatestByClientId: ");
                throw;
            }
        }

        public bool IsConnected()
        {
            var connStatus = this._client.PingAsync();
            connStatus.Wait();
            if (!connStatus.Result)
                _logger.LogWarning("InfluxDB - IsConnected: No DB connection...");
            return connStatus.Result;
        }

        ~InfluxDBService()
        {
            this._client.Dispose();
        }
    }
}
