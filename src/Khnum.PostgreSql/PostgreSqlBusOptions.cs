using System;

namespace Khnum.PostgreSql
{
    public class PostgreSqlBusOptions
    {
        public string ConnectionString { get; set; }
        public string Schema { get; set; } = "khnum";
        public TimeSpan SleepTime { get; set; } = TimeSpan.FromMilliseconds(200);
        public TimeSpan RetentionPeriod { get; set; } = TimeSpan.FromDays(7);
    }
}
