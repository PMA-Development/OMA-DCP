using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Serilog;
using Microsoft.Extensions.DependencyInjection;
using System.Reflection;
using DCP_App.Services.Interfaces;
using DCP_App.Services;
using DCP_App.Utils;

namespace DCP_App
{
    internal class Program
    {
        private static readonly CancellationTokenSource _cancellationTokenSource = new CancellationTokenSource();
        static void Main(string[] args)
        {
            Console.CancelKeyPress += (sender, e) =>
            {
                // We'll stop the process manually by using the CancellationToken
                e.Cancel = true;

                // Change the state of the CancellationToken to "Canceled"
                // - Set the IsCancellationRequested property to true
                // - Call the registered callbacks
                _cancellationTokenSource.Cancel();
            };

            ConfigurationBuilder builder = new ConfigurationBuilder();
            BuildConfig(builder);

            IConfiguration config = builder.Build();

            // ClientId is not allowed to be empty!
            if (config.GetValue<string>("ClientId", string.Empty) == string.Empty)
                throw new ArgumentException("Missing ClientId, please provide a ClientId!");

            Log.Logger = new LoggerConfiguration()
                .ReadFrom.Configuration(config)
                .CreateLogger();

            Log.Logger.Information("Application Starting");

            DeviceInfo.Id = config.GetValue<string>("ClientId")!;
            DeviceInfo.TurbineId = config.GetValue<string>("ClientId")!;

            var host = Host.CreateDefaultBuilder()
                .ConfigureServices((context, services) =>
                {
                    services.AddSingleton(_cancellationTokenSource);
                    services.AddSingleton<MqttConsumerService>();
                    services.AddSingleton<MqttProviderService>();

                    services.AddSingleton<IInfluxDBService, InfluxDBService>();
                })
                .UseSerilog()
                .Build();

            host.Services.GetService<MqttConsumerService>()!.Run();
            host.Services.GetService<MqttProviderService>()!.Run();

            // Keep the applikation running until cancelled.
            while (!_cancellationTokenSource.Token.IsCancellationRequested) { };

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
