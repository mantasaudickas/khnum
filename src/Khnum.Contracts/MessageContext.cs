using System;

namespace Khnum.Contracts
{
    public interface IMessageContext<TMessage>
    {
        TMessage Body { get; }
        IServiceProvider Services { get; }
    }
}
