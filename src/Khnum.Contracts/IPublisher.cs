using System.Threading.Tasks;

namespace Khnum.Contracts
{
    public interface IPublisher
    {
        Task PublishAsync<TMessage>(TMessage message);
    }
}
