using System;

namespace Khnum.Contracts
{
    public interface IKhnumScopeFactory
    {
        IKhnumScope CreateScope();
    }

    public interface IKhnumScope: IDisposable
    {
        IServiceProvider Services { get; }
    }
}
