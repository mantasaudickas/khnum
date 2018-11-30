namespace Khnum.PostgreSql.Entities
{
    public enum MessageState
    {
        Queued,
        Processing,
        Completed,
        Failed
    }
}
