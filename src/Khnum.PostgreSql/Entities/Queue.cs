namespace Khnum.PostgreSql.Entities
{
    public class Queue
    {
        public int QueueId { get; set; }
        public string Name { get; set; }
        public string RoutingKey { get; set; }
    }
}
