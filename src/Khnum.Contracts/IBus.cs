using System;
using System.Threading.Tasks;

namespace Khnum.Contracts
{
    public interface IBus: IDisposable
    {
        Task PublishAsync<TMessage>(TMessage message);
        void RegisterCallback(string consumerId, string queueName, string routingKey, Func<IBusMessage, Task> callback);
        Task StartReceivers();
    }
}
