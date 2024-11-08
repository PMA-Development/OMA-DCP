﻿using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Serilog;
using Microsoft.Extensions.DependencyInjection;
using DCP_App.Services.InfluxDB;
using DCP_App.Services.Mqtt;
using System.Reflection;

namespace DCP_App
{
    internal class Program
    {
        static void Main(string[] args)
        {
            // Define the cancellation token.
            CancellationTokenSource cts = new CancellationTokenSource();
            CancellationToken token = cts.Token;

            Console.CancelKeyPress += (sender, e) =>
            {
                // We'll stop the process manually by using the CancellationToken
                e.Cancel = true;

                // Change the state of the CancellationToken to "Canceled"
                // - Set the IsCancellationRequested property to true
                // - Call the registered callbacks
                cts.Cancel();
            };

            ConfigurationBuilder builder = new ConfigurationBuilder();
            BuildConfig(builder);

            IConfiguration config = builder.Build();

            // ClientId is not allowed to be empty!
            if (config["ClientId"] == string.Empty)
                config["ClientId"] = Guid.NewGuid().ToString();

            Log.Logger = new LoggerConfiguration()
                .ReadFrom.Configuration(config)
                .CreateLogger();

            Log.Logger.Information("Application Starting");

            var host = Host.CreateDefaultBuilder()
                .ConfigureServices((context, services) =>
                {
                    services.AddSingleton<MqttConsumerService, MqttConsumerService>();
                    services.AddSingleton<MqttProviderService, MqttProviderService>();

                    services.AddSingleton<IInfluxDBService, InfluxDBService>();
                })
                .UseSerilog()
                .Build();

            var mqttConsumerClient = host.Services.GetService<MqttConsumerService>();

            if (mqttConsumerClient != null ) 
                _ = mqttConsumerClient.StartWorker(token);

            // Do not run the provider client, if the provider is not enabled
            if (Convert.ToBoolean(config["MqttProvider:Enabled"]))
            {
                var mqttProviderClient = host.Services.GetService<MqttProviderService>();
                if (mqttProviderClient != null)
                    _ = mqttProviderClient.StartWorker(token);
            }

            // Keep the applikation running until cancelled.
            while (!token.IsCancellationRequested) { };

        }

        static void BuildConfig(IConfigurationBuilder builder)
        {
            builder.SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
                .AddJsonFile($"appsettings.{Environment.GetEnvironmentVariable("ASPNETCORE_ENVIRONMENT") ?? "Production"}.json", optional: true)
                .AddUserSecrets(Assembly.GetExecutingAssembly(), true)
                .AddEnvironmentVariables();
        }
    }
}
