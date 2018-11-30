using System;
using System.Threading.Tasks;

namespace Khnum.Contracts
{
    public interface IBus: IDisposable
    {
        Task SendAsync<TMessage>(TMessage message);
        void Receive(string queueName, string routingKey, Func<IBusMessage, Task> callback);
        Task Start();
    }
}
