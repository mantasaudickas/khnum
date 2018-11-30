using System;
using System.Collections.Generic;
using Khnum.Contracts;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Khnum
{
    public class ConsumerRegistry: IConsumerRegistry
    {
        private readonly IServiceCollection _services;

        private readonly List<IConsumerRegistration> _registrations = new List<IConsumerRegistration>();

        public ConsumerRegistry(IServiceCollection services)
        {
            _services = services;
        }


        public IConsumerRegistry Subscribe<TMessage, TConsumer>(string consumerId) where TConsumer: class, IConsumer<TMessage>
        {
            _services.TryAddTransient<TConsumer>();
            return Subscribe(consumerId, provider => provider.GetRequiredService<TConsumer>());
        }

        public IConsumerRegistry Subscribe<TMessage>(string consumerId, Func<IServiceProvider, IConsumer<TMessage>> createConsumer)
        {
            if (string.IsNullOrWhiteSpace(consumerId)) throw new ArgumentNullException(nameof(consumerId));
            consumerId = consumerId.ToLower();

            _registrations.Add(new ConsumerRegistration<TMessage>(consumerId, createConsumer));

            return this;
        }

        public IConsumerService CreateService(IBus bus)
        {
            return new ConsumerService(bus, _registrations);
        }
    }
}
