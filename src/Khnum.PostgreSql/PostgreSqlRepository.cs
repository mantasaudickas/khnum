using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Dapper;
using Khnum.PostgreSql.Entities;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace Khnum.PostgreSql
{
    internal class PostgreSqlRepository
    {
        private readonly string _schemaName;
        private readonly ILogger _logger;

        public PostgreSqlRepository(string schemaName, ILogger logger)
        {
            _schemaName = schemaName;
            _logger = logger;
        }

        public Task SqlCreateOsspExtension(NpgsqlConnection connection)
        {
            string sql = "CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\"";
            _logger.LogDebug("Creating UUID-OSSP extension: {Sql}", sql);
            return connection.ExecuteAsync(sql);
        }

        public Task SqlCreateSchema(NpgsqlConnection connection)
        {
            string sql = $"CREATE SCHEMA IF NOT EXISTS {_schemaName}";
            _logger.LogDebug("Creating schema: {Sql}", sql);
            return connection.ExecuteAsync(sql);
        }

        public Task SqlCreateQueueSequence(NpgsqlConnection connection)
        {
            string sql = $"CREATE SEQUENCE IF NOT EXISTS {_schemaName}.SEQ_QueueId";
            return connection.ExecuteAsync(sql);
        }

        public Task SqlCreateQueues(NpgsqlConnection connection)
        {
            string sql = $" CREATE TABLE IF NOT EXISTS {_schemaName}.queues (" +
                         $" QueueId INT NOT NULL CONSTRAINT PK_QueueId PRIMARY KEY DEFAULT nextval('{_schemaName}.SEQ_QueueId')," +
                         "  Name VARCHAR(1024) NOT NULL," +
                         "  RoutingKey VARCHAR(1024) NOT NULL," +
                         "  CONSTRAINT UC_Queues_Name UNIQUE (Name));";
            _logger.LogDebug("Creating Queues table: {Sql}", sql);
            return connection.ExecuteAsync(sql);
        }

        public Task SqlCreateQueueMessages(NpgsqlConnection connection)
        {
            string sql = $" CREATE TABLE IF NOT EXISTS {_schemaName}.queuemessages (" +
                         "  QueueMessageId UUID NOT NULL CONSTRAINT PK_QueueMessageId PRIMARY KEY DEFAULT uuid_generate_v4()," +
                         "  QueueId INT NOT NULL," +
                         "  Body TEXT NOT NULL," +
                         "  Properties TEXT NOT NULL," +
                         "  State VARCHAR(10) NOT NULL," +
                         "  StateDescription TEXT NULL," +
                         "  StateProcessor VARCHAR(1024) NULL," +
                         "  Created TIMESTAMP NOT NULL DEFAULT clock_timestamp()," +
                         "  Updated TIMESTAMP NOT NULL DEFAULT clock_timestamp()," +
                         $" CONSTRAINT FK_QueueMessages_Queue FOREIGN KEY (QueueId) REFERENCES {_schemaName}.queues (QueueId) ON DELETE CASCADE);";
            _logger.LogDebug("Creating QueueMessages table: {Sql}", sql);
            return connection.ExecuteAsync(sql);
        }

        public Task<QueueMessage> FetchQueueMessageAsync(NpgsqlConnection connection, Guid queueMessageId)
        {
            string sql = "  SELECT queuemessageid, queueid, body, properties, state, statedescription, stateprocessor, created, updated " +
                         $" FROM {_schemaName}.queuemessages " +
                         "  WHERE queuemessageid = :queueMessageId";

            return connection.QueryFirstOrDefaultAsync<QueueMessage>(sql, new {queueMessageId});
        }

        public Task<Guid?> FetchNextQueueMessageId(NpgsqlConnection connection, Queue queue, string stateProcessor)
        {
            var sql = $" UPDATE {_schemaName}.queuemessages" +
                      "  SET state = :processingState, Updated = clock_timestamp(), StateProcessor = :stateProcessor" +
                      "  WHERE queuemessageid = (" +
                      $"    select queuemessageid from {_schemaName}.queuemessages WHERE QueueId = :queueId AND State = :queuedState ORDER BY created ASC LIMIT 1 FOR UPDATE SKIP LOCKED)" +
                      "  RETURNING queuemessageid";

            return connection.ExecuteScalarAsync<Guid?>(sql, new
            {
                queueId = queue.QueueId,
                stateProcessor,
                processingState = MessageState.Processing.ToString(),
                queuedState = MessageState.Queued.ToString()
            });
        }

        public Task UpdateMessageState(NpgsqlConnection connection, Guid queueMessageId, string stateDescription, MessageState messageState)
        {
            var sql = $"UPDATE {_schemaName}.queuemessages " +
                      " SET state =:messageState, Updated = clock_timestamp(), StateDescription = :stateDescription " +
                      " WHERE queuemessageid = :queueMessageId";

            return connection.QueryAsync(sql, new
            {
                messageState = messageState.ToString(),
                stateDescription,
                queueMessageId
            });
        }

        public Task<IEnumerable<Queue>> FetchQueues(NpgsqlConnection connection, string routingKey)
        {
            string sql = $"SELECT queueid, name, routingkey " +
                         $"FROM {_schemaName}.queues " +
                         $"WHERE routingkey = :routingKey";

            return connection.QueryAsync<Queue>(sql, new {routingKey});
        }

        public Task InsertNewMessage(NpgsqlConnection connection, Queue queue, string body, string propertyBody)
        {
            string sql = $"INSERT INTO {_schemaName}.queuemessages (QueueId, Body, Properties, State) " +
                         " VALUES (:queueId, :body, :propertyBody, :queuedState);";

            return connection.ExecuteAsync(sql, new
            {
                queueId = queue.QueueId,
                body,
                propertyBody,
                queuedState = MessageState.Queued.ToString()
            });
        }

        public Task InsertQueue(NpgsqlConnection connection, string queueName, string routingKey)
        {
            var sql = $"INSERT INTO {_schemaName}.queues(Name, RoutingKey) VALUES(:queueName, :routingKey) ON CONFLICT (Name) DO NOTHING";
            return connection.ExecuteAsync(sql, new
            {
                queueName,
                routingKey
            });
        }

        public Task<IEnumerable<Queue>> FetchQueue(NpgsqlConnection connection, string queueName)
        {
            return connection.QueryAsync<Queue>($"SELECT QueueId, Name FROM {_schemaName}.queues WHERE Name = :queueName", new {queueName});
        }
    }
}
