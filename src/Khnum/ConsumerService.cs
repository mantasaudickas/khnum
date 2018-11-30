using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Khnum.Contracts;

namespace Khnum
{
    public class ConsumerService: IConsumerService
    {
        private readonly IBus _bus;
        private readonly IList<IConsumerRegistration> _registrations;

        public ConsumerService(IBus bus, IList<IConsumerRegistration> registrations)
        {
            _bus = bus;
            _registrations = registrations;
        }

        public Task StartConsumersAsync(IServiceProvider services)
        {
            if (_registrations.Count == 0)
                return Task.CompletedTask;

            foreach (var registration in _registrations)
            {
                var queueName = $"{registration.MessageType}:{registration.ConsumerId}".ToLower();
                var routingkey = $"{registration.MessageType}".ToLower();
                _bus.RegisterCallback(queueName, routingkey, message => registration.ConsumeAsync(services, message.Body, message.Properties));
            }

            return _bus.StartReceivers();
        }

        public void Dispose()
        {
            _bus.Dispose();
        }
    }
}
