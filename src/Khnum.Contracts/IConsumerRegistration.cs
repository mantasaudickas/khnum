using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Khnum.Contracts
{
    public interface IConsumerRegistration
    {
        string ConsumerId { get; }
        Type MessageType { get; }

        Task ConsumeAsync(IServiceProvider services, byte[] message, IDictionary<string, object> properties);
    }
}