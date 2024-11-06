using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using MQTTnet.Server;
using MQTTnet;
using Serilog;
using MQTTnet.Internal;
using MQTTnet.Client;
using System.Security.Authentication;
using System.Security.Cryptography.X509Certificates;
using MQTTnet.Protocol;
using System.Text;
using MQTTnet.Formatter;
using Microsoft.Extensions.DependencyInjection;
using DCP_App.Services.InfluxDB;
using DCP_App.Services.Mqtt;

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

            Log.Logger = new LoggerConfiguration()
                .ReadFrom.Configuration(builder.Build())
                .Enrich.FromLogContext()
                .WriteTo.Console()
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

            var mqttWorkerClient = host.Services.GetService<MqttConsumerService>();
            var mqttProviderClient = host.Services.GetService<MqttProviderService>();

            mqttWorkerClient.StartWorker(token);
            mqttProviderClient.StartWorker(token);


            //MqttCollectData.Subscribe_Topic(token);


            //MqttCollectData.SubscribeAsync().Wait();

            while (!token.IsCancellationRequested) { };

        }

        static void BuildConfig(IConfigurationBuilder builder)
        {
            builder.SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
                .AddJsonFile($"appsettings.{Environment.GetEnvironmentVariable("ASPNETCORE_ENVIRONMENT") ?? "Production"}.json", optional: true)
                .AddEnvironmentVariables();


        }
    }

    public static class MqttCollectData
    {
        private static readonly ILogger _logger = Log.ForContext(typeof(MqttCollectData));

        public static async Task SubscribeAsync()
        {
            string broker = "localhost";
            int port = 1883;
            string clientId = "DCP";
            string topic = "/test";
            string username = "DCP";
            string password = "DCP";

            // Create a MQTT client factory
            var factory = new MqttFactory();

            // Create a MQTT client instance
            var mqttClient = factory.CreateMqttClient();

            // Create MQTT client options
            var options = new MqttClientOptionsBuilder()
            .WithTcpServer(broker, port) // MQTT broker address and port
            .WithCredentials(username, password) // Set username and password
            .WithClientId(clientId)
            .WithCleanSession()
            .Build();


            // Connect to MQTT broker
            var connectResult = await mqttClient.ConnectAsync(options);

            if (connectResult.ResultCode == MqttClientConnectResultCode.Success)
            {
                Console.WriteLine("Connected to MQTT broker successfully.");

                // Subscribe to a topic
                await mqttClient.SubscribeAsync(topic);

                // Callback function when a message is received
                mqttClient.ApplicationMessageReceivedAsync += e =>
                {
                    Console.WriteLine($"Received message: {Encoding.UTF8.GetString(e.ApplicationMessage.PayloadSegment)}");
                    return Task.CompletedTask;
                };

                // Publish a message 10 times
                for (int i = 0; i < 10; i++)
                {
                    var message = new MqttApplicationMessageBuilder()
                        .WithTopic(topic)
                        .WithPayload($"Hello, MQTT! Message number {i}")
                        .WithQualityOfServiceLevel(MqttQualityOfServiceLevel.AtLeastOnce)
                        .WithRetainFlag()
                        .Build();

                    await mqttClient.PublishAsync(message);
                    await Task.Delay(2000); // Wait for 1 second
                }

                // Unsubscribe and disconnect
                await mqttClient.UnsubscribeAsync(topic);
                await mqttClient.DisconnectAsync();
            }
            else
            {
                Console.WriteLine($"Failed to connect to MQTT broker: {connectResult.ResultCode}");
            }
        }

        public static async Task Subscribe_Topic(CancellationToken shutdownToken)
        {
            /*
             * This sample subscribes to a topic.
             */

            string broker = "localhost";
            int port = 1883;
            string clientId = "DCP";
            string topic = "/test";
            string username = "DCP";
            string password = "DCP";

            var mqttFactory = new MqttFactory();
            var mqttClient = mqttFactory.CreateMqttClient();

            try
            {
                var mqttClientOptions = new MqttClientOptionsBuilder()
                    .WithTcpServer(broker, port) // MQTT broker address and port
                    .WithCredentials(username, password) // Set username and password
                    .WithClientId(clientId)
                    .WithProtocolVersion(MQTTnet.Formatter.MqttProtocolVersion.V500)
                    //.WithCleanSession()
                    .Build();

                mqttClient.ApplicationMessageReceivedAsync += e =>
                {
                    Console.WriteLine($"Received message: {Encoding.UTF8.GetString(e.ApplicationMessage.PayloadSegment)}");
                    return Task.CompletedTask;
                };

                var mqttTopicFilter = new MqttTopicFilterBuilder()
                    .WithTopic(topic)
                    .WithAtLeastOnceQoS()
                    .Build();

                var mqttSubscribeOptions = mqttFactory.CreateSubscribeOptionsBuilder()
                    .WithTopicFilter(mqttTopicFilter)
                    .Build();

                // Handle reconnection logic and cancellation token properly
                while (!shutdownToken.IsCancellationRequested)
                {
                    try
                    {
                        // Periodically check if the connection is alive, otherwise reconnect
                        if (!await mqttClient.TryPingAsync())
                        {
                            _logger.Information("Attempting to connect to MQTT Broker...");
                            await mqttClient.ConnectAsync(mqttClientOptions, shutdownToken);

                            // Subscribe every time we connect, but keep using the same OptionsBuilder, to avoid subscribing more than once.
                            await mqttClient.SubscribeAsync(mqttSubscribeOptions, shutdownToken);
                            _logger.Information("MQTT client subscribed to topic.");
                        }
                    }
                    catch (Exception ex)
                    {
                        _logger.Error(ex, "An error occurred during MQTT operation.");
                    }

                    // Check the connection status every 5 seconds
                    await Task.Delay(TimeSpan.FromSeconds(5), shutdownToken);
                }

                _logger.Information("Cancellation requested. Exiting...");
            }
            catch (Exception ex)
            {
                _logger.Error(ex, "Connection failed.");
            }
            finally
            {
                // Dispose of the MQTT client manually at the end
                if (mqttClient.IsConnected)
                {
                    await mqttClient.DisconnectAsync();
                }

                mqttClient.Dispose();
            }
        }

        // TODO: Implement this logic in the subscribtion.
        static void ConcurrentProcessingWithLimit(CancellationToken shutdownToken, IMqttClient mqttClient)
        {
            /*
             * This sample shows how to achieve concurrent processing, with:
             * - a maximum concurrency limit based on Environment.ProcessorCount
             */

            //var concurrent = new SemaphoreSlim(Environment.ProcessorCount);
            var concurrent = new SemaphoreSlim(2);

            mqttClient.ApplicationMessageReceivedAsync += async ea =>
            {
                await concurrent.WaitAsync(shutdownToken).ConfigureAwait(false);

                async Task ProcessAsync()
                {
                    try
                    {
                        // DO YOUR WORK HERE!
                        await Task.Delay(1000, shutdownToken);
                    }
                    finally
                    {
                        concurrent.Release();
                    }
                }

                _ = Task.Run(ProcessAsync, shutdownToken);
            };
        }
    }
}
