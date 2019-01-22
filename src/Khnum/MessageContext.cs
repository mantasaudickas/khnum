using System;
using Khnum.Contracts;

namespace Khnum
{
    public class MessageContext<TMessage> : IMessageContext<TMessage>
    {
        public MessageContext(TMessage body, IServiceProvider services)
        {
            Body = body;
            Services = services;
        }

        public TMessage Body { get; }
        public IServiceProvider Services { get; }
    }
}
