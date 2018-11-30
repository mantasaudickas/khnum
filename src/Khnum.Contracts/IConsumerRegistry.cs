using System;

namespace Khnum.Contracts
{
    public interface IConsumerRegistry
    {
        IConsumerRegistry Subscribe<TMessage, TConsumer>(string consumerId) where TConsumer: class, IConsumer<TMessage>;
        IConsumerRegistry Subscribe<TMessage>(string consumerId, Func<IServiceProvider, IConsumer<TMessage>> createConsumer);
        IConsumerService CreateService(IBus bus);
    }
}
