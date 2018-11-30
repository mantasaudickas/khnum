using System;

namespace Khnum.PostgreSql.Entities
{
    public class QueueMessage
    {
        public Guid QueueMessageId { get; set; }
        public int QueueId { get; set; }
        public string Body { get; set; }
        public string Properties { get; set; }
        public string State { get; set; }
        public string StateDescription { get; set; }
        public string StateProcessor { get; set; }
        public DateTime Created { get; set; }
        public DateTime Updated { get; set; }
    }
}
