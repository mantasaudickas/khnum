using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Khnum.Contracts;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Utf8Json;

namespace Khnum
{
    public class ConsumerRegistration<TMessage>: IConsumerRegistration
    {
        private readonly Func<IServiceProvider, IConsumer<TMessage>> _createConsumer;

        public string ConsumerId { get; }
        public Type MessageType { get; }

        public ConsumerRegistration(string consumerId, Func<IServiceProvider, IConsumer<TMessage>> createConsumer)
        {
            ConsumerId = consumerId;
            MessageType = typeof(TMessage);
            _createConsumer = createConsumer;
        }

        public Task ConsumeAsync(IServiceProvider services, byte[] message, IDictionary<string, object> properties)
        {
            var body = JsonSerializer.Deserialize<TMessage>(message);

            using (var scope = services.CreateScope())
            {
                var context = new MessageContext<TMessage>(body, scope.ServiceProvider);

                using (CreateLoggerScope(scope.ServiceProvider, properties))
                {
                    var consumer = _createConsumer(scope.ServiceProvider);
                    return consumer.ConsumeAsync(context);
                }
            }
        }

        private IDisposable CreateLoggerScope(IServiceProvider services, IDictionary<string, object> properties)
        {
            if (properties == null)
                return null;

            var logger = services.GetService<ILogger<TMessage>>();
            return logger?.BeginScope(properties.ToArray());
        }
    }
}
