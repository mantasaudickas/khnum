using System;
using Khnum.Contracts;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Khnum.PostgreSql
{
    public static class ServiceCollectionExtensions
    {
        public static IServiceCollection AddKhnumPostgreSqlServiceBus(this IServiceCollection collection, PostgreSqlBusOptions options, Action<IConsumerRegistry> subscribe)
        {
            collection.AddKhnumServiceBus(subscribe);
            collection.AddSingleton(options);
            collection.TryAddSingleton<IBus, PostgreSqlBus>();
            collection.TryAddSingleton<IPublisher, PostgreSqlPublisher>();
            return collection;
        }
    }
}
