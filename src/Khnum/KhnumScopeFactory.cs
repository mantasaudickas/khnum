using System;
using Khnum.Contracts;
using Microsoft.Extensions.DependencyInjection;

namespace Khnum
{
    public class KhnumScopeFactory: IKhnumScopeFactory
    {
        private readonly IServiceProvider _services;

        public KhnumScopeFactory(IServiceProvider services)
        {
            _services = services;
        }

        public IKhnumScope CreateScope()
        {
            IServiceScope serviceScope = _services.CreateScope();
            return new DefaultKhnumScope(serviceScope);
        }
    }

    public class DefaultKhnumScope : IKhnumScope
    {
        private readonly IServiceScope _scope;

        public DefaultKhnumScope(IServiceScope scope)
        {
            _scope = scope;
        }

        public IServiceProvider Services => _scope.ServiceProvider;

        public void Dispose()
        {
            _scope?.Dispose();
        }
    }
}
