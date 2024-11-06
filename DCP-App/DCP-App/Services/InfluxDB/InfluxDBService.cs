using InfluxDB.Client.Writes;
using InfluxDB.Client;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using DCP_App.Services.Mqtt;
using DCP_App.Entities;
using InfluxDB.Client.Linq;
using InfluxDB.Client.Api.Domain;
using DCP_App.Measurements;
using Microsoft.Extensions.Hosting;
using Newtonsoft.Json.Linq;
using System.Net.Sockets;
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
            await this._client.GetWriteApiAsync(this._converter)
                .WriteMeasurementsAsync(sensorEntities, WritePrecision.S);
        }

        public List<SensorEntity> ReadAll()
        {
            var queryApi = this._client!.GetQueryApiSync(this._converter);
            //
            // Select ALL
            //
            var query = from s in InfluxDBQueryable<SensorEntity>.Queryable(this._bucket, this._org, queryApi, this._converter)
                        select s;
            Console.WriteLine("==== Select ALL ====");
            return query.ToList();
        }


        ~InfluxDBService()
        {
            this._client.Dispose();
        }
    }
}
