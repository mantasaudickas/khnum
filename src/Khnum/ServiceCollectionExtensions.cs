using System;
using Khnum.Contracts;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Khnum
{
    public static class ServiceCollectionExtensions
    {
        public static IServiceCollection AddKhnumServiceBus<TBus, TPublisher, TBusOptions>(this IServiceCollection collection, TBusOptions busOptions, Action<IConsumerRegistry> subscribe)
            where TBus: class, IBus
            where TPublisher: class, IPublisher
            where TBusOptions: class
        {
            if (subscribe != null)
            {
                var registry = new ConsumerRegistry(collection);
                subscribe(registry);
                collection.AddSingleton<IConsumerRegistry>(registry);
            }

            collection.TryAddSingleton(busOptions);
            collection.TryAddSingleton<IBus, TBus>();
            collection.TryAddSingleton<IPublisher, TPublisher>();
            collection.TryAddSingleton<ISerializer, Serializer>();
            collection.TryAddSingleton<IKhnumScopeFactory, KhnumScopeFactory>();
            return collection;
        }

        public static IApplicationBuilder UseKhnumServiceBus(this IApplicationBuilder app)
        {
            var services = app.ApplicationServices;

            var registry = services.GetService<IConsumerRegistry>();
            if (registry != null)
            {
                var lifetime = services.GetRequiredService<IApplicationLifetime>();
                var bus = services.GetRequiredService<IBus>();
                var consumers = registry.CreateService(bus);
                lifetime.ApplicationStarted.Register(async () => await consumers.StartConsumersAsync(app.ApplicationServices).ConfigureAwait(false));
                lifetime.ApplicationStopping.Register(() => consumers.Dispose());
            }

            return app;
        }
    }
}
