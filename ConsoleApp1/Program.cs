namespace ConsoleApp1
{
    using System;
    using System.Diagnostics;
    using System.Text.Json;
    using EventStore.Client;

    internal class Program
    {
        static async Task Main(string[] args){
            AppDomain.CurrentDomain.FirstChanceException += (sender, eventArgs) => { Console.WriteLine($"[FirstChance] - {eventArgs.Exception.ToString()} - sender is {sender?.GetType()}"); };

            const String connectionString = "esdb://admin:changeit@192.168.10.90:2113?tls=false&tlsVerifyCert=false";
            var settings = EventStoreClientSettings.Create(connectionString);
            var client = new EventStoreClient(settings);

            String streamName = $"some-stream";
            String groupName = $"subscription-group";

            var evt = new{
                             EntityId = Guid.NewGuid().ToString("N"),
                             ImportantData = "I wrote my first event!"
                         };


            IDictionary<String, String> metadata = new Dictionary<String, String>();

            ActivityTraceId activityId = ActivityTraceId.CreateRandom();
            ActivitySpanId activitySpanId = ActivitySpanId.CreateRandom();

            metadata.Add("$traceId", activityId.ToHexString());
            metadata.Add("$spanId", activitySpanId.ToHexString());

            EventData eventData = new(
                                      Uuid.NewUuid(),
                                      "TestEvent",
                                      JsonSerializer.SerializeToUtf8Bytes(evt),
                                      JsonSerializer.SerializeToUtf8Bytes(metadata)
                                     );

            await client.AppendToStreamAsync(
                                             streamName,
                                             StreamState.Any,
                                             new[] { eventData },
                                             cancellationToken: CancellationToken.None
                                            );




            UserCredentials userCredentials = new("admin", "changeit");
            EventStorePersistentSubscriptionsClient persistentSubscriptionsClient = new(EventStoreClientSettings.Create(connectionString));

            PersistentSubscriptionSettings persistentSubscriptionSettings = new(true, 
                                                                                new StreamPosition(0), 
                                                                                maxRetryCount: 0);


            try
            {
                await persistentSubscriptionsClient.DeleteToStreamAsync(streamName, groupName);
            }
            catch(Exception e){
                Console.WriteLine(e.Message);
            }

            await persistentSubscriptionsClient.CreateToStreamAsync(
                                                                    streamName,
                                                                    groupName,
                                                                    persistentSubscriptionSettings,
                                                                    userCredentials:userCredentials
                                                                   );

            await Task.Delay(TimeSpan.FromSeconds(1));

            await persistentSubscriptionsClient.SubscribeToStreamAsync(streamName,
                                                                       groupName,
                                                                       EventAppeared,
                                                                       SubscriptionDropped,
                                                                       userCredentials,
                                                                       10,
                                                                       CancellationToken.None);

            Console.ReadKey();
        }

        private static void SubscriptionDropped(PersistentSubscription arg1, SubscriptionDroppedReason arg2, Exception? arg3){
            Console.WriteLine($"{arg2}");
        }

        private static Task EventAppeared(PersistentSubscription arg1, ResolvedEvent arg2, Int32? arg3, CancellationToken arg4){
            Console.WriteLine($"Event Appeared from {arg2.OriginalStreamId}");

            arg1.Ack(arg2);

            return Task.CompletedTask;
        }
    }


}