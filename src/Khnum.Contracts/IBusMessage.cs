using System;
using System.Collections.Generic;

namespace Khnum.Contracts
{
    public interface IBusMessage
    {
        string MessageId { get; }
        byte[] Body { get; }
        IDictionary<string, object> Properties { get; }
        IServiceProvider ServiceProvider { get; }
    }
}
