using System;
using System.Threading.Tasks;
using Khnum.Contracts;
using Khnum.Tests.Contracts;

namespace Khnum.Publisher
{
    public class SendTimeRequestConsumer: IConsumer<SendTimeRequest>
    {
        public Task ConsumeAsync(IMessageContext<SendTimeRequest> message)
        {
            Console.WriteLine("Received: {0}", message.Body.Time);
            return Task.CompletedTask;
        }
    }
}
