using System.Threading.Tasks;

namespace Khnum.Contracts
{
    public interface IConsumer<TMessage>
    {
        Task ConsumeAsync(IMessageContext<TMessage> message);
    }
}
