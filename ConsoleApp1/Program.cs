using System.Diagnostics;
using OpenTelemetry;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;

namespace ConsoleApp1
{
    using System;
    using System.Text.Json;
    using EventStore.Client;

    internal class Program
    {
        static async Task Main(string[] args)
        {
            using var tracerProvider = Sdk.CreateTracerProviderBuilder()
                .SetResourceBuilder(ResourceBuilder.CreateDefault().AddService("MySample"))
                .AddSource("eventstoredb")
                .AddConsoleExporter()
                .Build();
            var listener = new DiagnosticListener("eventstoredb");

            const String connectionString = "esdb://admin:changeit@localhost:2113?tls=false&tlsVerifyCert=false";
            var settings = EventStoreClientSettings.Create(connectionString);
            var client = new EventStoreClient(settings);

            String streamName = $"test-stream-0";

            var failingMetadata = new ReadOnlyMemory<byte>("\n\u0018OrganizationCreatedEvent"u8.ToArray());
            var okMetadata = JsonSerializer.SerializeToUtf8Bytes(new Dictionary<string, string>() { { "test", "test" } });

            var metadata = failingMetadata;

            var cancel = new CancellationTokenSource();
            var producerTask = Task.Run(async () =>
            {
                try
                {
                    while (true)
                    {
                        await Task.Delay(5000, cancel.Token);
                        Console.WriteLine("===================");
                        Console.WriteLine("===================");
                        Console.WriteLine("Sending new event");
                        EventData eventData = new(Uuid.NewUuid(), "TestEvent", metadata, metadata, "application/octet-stream");
                        await client.AppendToStreamAsync(
                                                         streamName,
                                                         StreamState.Any,
                                                         new[] { eventData },
                                                         cancellationToken: CancellationToken.None
                                                        );
                    }
                }
                catch (TaskCanceledException e)
                {
                }
            }, cancel.Token);

            UserCredentials userCredentials = new("admin", "changeit");

            try
            {
                var subscription = client.SubscribeToStream(streamName, FromStream.Start, true, userCredentials, CancellationToken.None);
                await foreach (var message in subscription.Messages.WithCancellation(cancel.Token))
                {
                    switch (message)
                    {
                        case StreamMessage.SubscriptionConfirmation(var id):
                            Console.WriteLine($"Confirmed {id}");
                            break;
                        case StreamMessage.Event(var evnt):
                            await EventAppeared(evnt);
                            break;
                        case StreamMessage.CaughtUp:
                            Console.WriteLine("Processing events live");
                            break;
                        case StreamMessage.FellBehind:
                            Console.WriteLine("Catching up");
                            break;
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("ERROR OCCURED-----------------------");
                Console.WriteLine(e.ToString());
                Console.WriteLine("------------------------------------");
            }

            await cancel.CancelAsync();
            listener.Dispose();
            await producerTask;

            Console.ReadKey();
        }

        private static Task EventAppeared(ResolvedEvent @event)
        {
            Console.WriteLine($"Event Appeared from {@event.OriginalStreamId}");
            return Task.CompletedTask;
        }
    }
}