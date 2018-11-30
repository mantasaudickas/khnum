using Khnum.Contracts;

namespace Khnum
{
    public class MessageContext<TMessage> : IMessageContext<TMessage>
    {
        public MessageContext(TMessage body)
        {
            Body = body;
        }

        public TMessage Body { get; }
    }
}