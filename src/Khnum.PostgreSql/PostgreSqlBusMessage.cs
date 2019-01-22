using System.Collections.Generic;
using Khnum.Contracts;

namespace Khnum.PostgreSql
{
    public class PostgreSqlBusMessage: IBusMessage
    {
        public string MessageId { get; }
        public byte[] Body { get; }
        public IDictionary<string, object> Properties { get; }

        public PostgreSqlBusMessage(string messageId, byte[] body, IDictionary<string, object> properties)
        {
            MessageId = messageId;
            Body = body;
            Properties = properties;
        }
    }
}
