namespace Khnum.Contracts
{
    public interface IMessageContext<TMessage>
    {
        TMessage Body { get; }
    }
}
