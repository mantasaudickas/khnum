using System;

namespace Khnum.PostgreSql.Exceptions
{
    public class PostgreSqlBusException: Exception
    {
        public PostgreSqlBusException(string message) : base(message)
        {
        }

        public PostgreSqlBusException(string message, Exception innerException) : base(message, innerException)
        {
        }
    }
}
