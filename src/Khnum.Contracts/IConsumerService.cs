using System;
using System.Threading.Tasks;

namespace Khnum.Contracts
{
    public interface IConsumerService: IDisposable
    {
        Task StartConsumersAsync(IServiceProvider services);
    }
}
