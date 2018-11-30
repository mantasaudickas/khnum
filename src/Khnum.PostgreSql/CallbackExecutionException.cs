using System;

namespace Khnum.PostgreSql
{
    public class CallbackExecutionException: Exception
    {
        public int CallbackNo { get; }
        public Guid MessageId { get; }

        public CallbackExecutionException(int callbackNo, Guid messageId, Exception exception) : base($"Callback {callbackNo} execution failed!", exception)
        {
            CallbackNo = callbackNo;
            MessageId = messageId;
        }
    }
}
