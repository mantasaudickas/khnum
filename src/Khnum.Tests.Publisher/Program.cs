using System;
using System.Threading.Tasks;
using Khnum.Contracts;
using Khnum.PostgreSql;
using Khnum.Publisher;
using Khnum.Tests.Contracts;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Khnum.Tests.Publisher
{
    class Program
    {
        static async Task Main(string[] args)
        {
            var collection = new ServiceCollection();
            collection.AddLogging(builder => builder.SetMinimumLevel(LogLevel.Debug).AddConsole());
            collection.AddKhnumServiceBus<PostgreSqlBus, PostgreSqlPublisher, PostgreSqlBusOptions>(
                new PostgreSqlBusOptions
                {
                    ConnectionString = "Server=127.0.0.1;Database=test;User Id=admin;Password=admin",
                    RetentionPeriod = TimeSpan.FromMinutes(15)
                },
                registry =>
                {
                    registry.Subscribe<SendTimeRequest, SendTimeRequestConsumer>("consumer-id-x");
                    registry.Subscribe<SendTimeRequest, SendTimeRequestConsumer>("consumer-id-y");
                });

            var services = collection.BuildServiceProvider(true);

            using (var consumer = await StartConsumer(services).ConfigureAwait(false))
            {
                while (true)
                {
                    Console.WriteLine("Press ESC to exit or any other key to publish");
                    var key = Console.ReadKey();
                    if (key.Key == ConsoleKey.Escape)
                        break;

                    var publisher = services.GetRequiredService<IPublisher>();

                    switch (key.Key)
                    {
                        case ConsoleKey.Q:
                            await publisher.PublishAsync(new FailProcessingRequest()).ConfigureAwait(false);
                            break;
                        default:
                            await publisher.PublishAsync(new SendTimeRequest {Time = DateTime.Now})
                                .ConfigureAwait(false);
                            break;
                    }
                }
            }

            Console.WriteLine("Press ENTER to exit");
            Console.ReadLine();
        }

        private static async Task<IConsumerService> StartConsumer(IServiceProvider services)
        {
            var registry = services.GetRequiredService<IConsumerRegistry>();
            var bus = services.GetRequiredService<IBus>();
            var service = registry.CreateService(bus);
            await service
                .StartConsumersAsync(services)
                .ConfigureAwait(false);
            return service;
        }
    }
}
