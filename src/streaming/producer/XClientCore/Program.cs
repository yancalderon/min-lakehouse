using System;
using System.Linq;
using System.Configuration;
using System.Reactive.Linq;
using System.Threading.Tasks;
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Producer;

namespace TwitterClient
{
    class Program
    {
        static async Task Main(string[] args)
        {
            try
            {
                // Solo necesitamos el Bearer Token y keywords para API v2
                var bearerToken = ConfigurationManager.AppSettings["bearer_token"];
                var keywords = ConfigurationManager.AppSettings["twitter_keywords"];

                if (string.IsNullOrEmpty(bearerToken))
                {
                    throw new Exception("Bearer Token is required for Twitter API v2");
                }

                // Configurar el cliente de Event Hub
                var producer = new EventHubProducerClient(
                    ConfigurationManager.AppSettings["EventHubConnectionString"],
                    ConfigurationManager.AppSettings["EventHubName"],
                    new EventHubProducerClientOptions()
                );

                Console.WriteLine($"Sending data eventhub : {producer.EventHubName} PartitionCount = {(await producer.GetPartitionIdsAsync()).Count()}");

                // Configurar el stream de Twitter usando solo Bearer Token
                var twitterConfig = new TwitterConfig(
                    oauthToken: null,        // No necesario para API v2
                    oauthTokenSecret: null,  // No necesario para API v2
                    oauthConsumerKey: null,  // No necesario para API v2
                    oauthConsumerSecret: null, // No necesario para API v2
                    keywords: keywords,
                    bearerToken: bearerToken
                );

                // Crear el stream de Twitter
                IObservable<string> twitterStream = TwitterStream.StreamStatuses(twitterConfig)
                    .ToObservable();

                // Configurar el tamaño máximo del mensaje y tiempo de buffer
                int maxMessageSizeInBytes = 250 * 1024;
                int maxSecondsToBuffer = 20;

                // Crear el observador de eventos
                IObservable<EventData> eventDataObserver = Observable.Create<EventData>(
                    outputObserver => twitterStream.Subscribe(
                        new EventDataGenerator(outputObserver, maxMessageSizeInBytes, maxSecondsToBuffer)));

                // Configurar el envío de eventos
                int maxRequestsInProgress = 5;
                IObservable<Task> sendTasks = eventDataObserver
                    .Select(async e =>
                    {
                        try
                        {
                            var batch = await producer.CreateBatchAsync();
                            if (!batch.TryAdd(e))
                            {
                                throw new ArgumentOutOfRangeException("Content too big to send in a single eventhub message");
                            }
                            await producer.SendAsync(batch);
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"Error sending to Event Hub: {ex.Message}");
                            throw;
                        }
                    })
                    .Buffer(TimeSpan.FromMinutes(1), maxRequestsInProgress)
                    .Select(sendTaskList => Task.WhenAll(sendTaskList));

                // Suscribirse al stream y manejar errores
                var subscription = sendTasks.Subscribe(
                    async sendEventDatasTask =>
                    {
                        try
                        {
                            await sendEventDatasTask;
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"Error in send task: {ex.Message}");
                        }
                    },
                    ex =>
                    {
                        Console.WriteLine($"Error in stream: {ex.Message}");
                        Console.WriteLine("Restarting stream in 5 seconds...");
                        Task.Delay(5000).Wait();
                        Main(args).Wait(); // Reintentar la conexión
                    });

                Console.WriteLine("Stream started. Press Ctrl+C to exit.");
                Console.CancelKeyPress += (s, e) =>
                {
                    Console.WriteLine("Shutting down...");
                    subscription.Dispose();
                    producer.DisposeAsync().AsTask().Wait();
                };

                // Mantener la aplicación corriendo
                await Task.Delay(-1);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Fatal error: {ex.Message}");
                Console.WriteLine($"Stack trace: {ex.StackTrace}");
                throw;
            }
        }
    }
}