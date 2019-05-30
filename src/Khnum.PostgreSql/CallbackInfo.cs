using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Khnum.Contracts;

namespace Khnum.PostgreSql
{
    internal class CallbackInfo
    {
        public string ConsumerId { get; set; }
        public string QueueName { get; set; }
        public string RoutingKey { get; set; }
        public  List<Func<IBusMessage, Task>> Callbacks { get; set; }
    }
}
