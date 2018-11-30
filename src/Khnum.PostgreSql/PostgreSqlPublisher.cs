using System.Threading.Tasks;
using Khnum.Contracts;

namespace Khnum.PostgreSql
{
    public class PostgreSqlPublisher: IPublisher
    {
        private readonly IBus _bus;

        public PostgreSqlPublisher(IBus bus)
        {
            _bus = bus;
        }

        public Task PublishAsync<TMessage>(TMessage message)
        {
            return _bus.PublishAsync(message);
        }
    }
}
