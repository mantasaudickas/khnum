using System;
using Khnum.Contracts;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.DependencyInjection;

namespace Khnum
{
    public static class ServiceCollectionExtensions
    {
        public static IServiceCollection AddKhnumServiceBus(this IServiceCollection collection, Action<IConsumerRegistry> subscribe)
        {

            if (subscribe != null)
            {
                var registry = new ConsumerRegistry(collection);
                subscribe(registry);
                collection.AddSingleton<IConsumerRegistry>(registry);
            }

            collection.AddSingleton<ISerializer, Serializer>();
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
