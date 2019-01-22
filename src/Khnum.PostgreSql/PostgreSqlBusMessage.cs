using System;
using System.Collections.Generic;
using Khnum.Contracts;

namespace Khnum.PostgreSql
{
    public class PostgreSqlBusMessage: IBusMessage
    {
        public string MessageId { get; }
        public byte[] Body { get; }
        public IDictionary<string, object> Properties { get; }
        public IServiceProvider ServiceProvider { get; }

        public PostgreSqlBusMessage(string messageId, byte[] body, IDictionary<string, object> properties, IServiceProvider serviceProvider)
        {
            MessageId = messageId;
            Body = body;
            Properties = properties;
            ServiceProvider = serviceProvider;
        }
    }
}
