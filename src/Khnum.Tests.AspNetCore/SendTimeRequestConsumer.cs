using System.Threading.Tasks;
using Khnum.Contracts;
using Khnum.Tests.Contracts;
using Microsoft.Extensions.Logging;

namespace Khnum.Tests.AspNetCore
{
    public class SendTimeRequestConsumer: IConsumer<SendTimeRequest>
    {
        private readonly ILogger<SendTimeRequestConsumer> _logger;

        public SendTimeRequestConsumer(ILogger<SendTimeRequestConsumer> logger)
        {
            _logger = logger;
        }

        public Task ConsumeAsync(IMessageContext<SendTimeRequest> message)
        {
            _logger.LogInformation("Received: {0}", message.Body.Time);
            return Task.CompletedTask;
        }
    }
}
