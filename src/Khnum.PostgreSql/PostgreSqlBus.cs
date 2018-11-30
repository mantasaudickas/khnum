using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Dapper;
using Khnum.Contracts;
using Khnum.PostgreSql.Entities;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace Khnum.PostgreSql
{
    public class PostgreSqlBus: IBus
    {
        private static readonly string MachineName = Dns.GetHostName();
        private readonly ConcurrentDictionary<string, CallbackInfo> _callbacks = new ConcurrentDictionary<string, CallbackInfo>();
        private readonly CancellationTokenSource _tokens = new CancellationTokenSource();

        private readonly PostgreSqlBusOptions _options;
        private readonly ISerializer _serializer;
        private readonly ILogger<PostgreSqlBus> _logger;
        private readonly string _schemaName;
        private readonly string _processorName;

        private class CallbackInfo
        {
            public string QueueName { get; set; }
            public string RoutingKey { get; set; }
            public  List<Func<IBusMessage, Task>> Callbacks { get; set; }
        }

        public PostgreSqlBus(PostgreSqlBusOptions options, ISerializer serializer, ILogger<PostgreSqlBus> logger)
        {
            _options = options;
            _serializer = serializer;
            _logger = logger;
            _schemaName = _options.Schema.ToLower();
            _processorName = $"{MachineName}:{Process.GetCurrentProcess().Id}";
        }

        public void Receive(string queueName, string routingKey, Func<IBusMessage, Task> callback)
        {
            queueName = queueName.ToLower();
            routingKey = routingKey.ToLower();

            var info = _callbacks.GetOrAdd(queueName, new CallbackInfo
            {
                QueueName = queueName,
                RoutingKey = routingKey,
                Callbacks = new List<Func<IBusMessage, Task>>()
            });

            info.Callbacks.Add(callback);
        }

        public async Task Start()
        {
            await InitializeSchemaAsync().ConfigureAwait(false);

            var queues = _callbacks.Keys.ToList();
            for (int i = 0; i < queues.Count; ++i)
            {
                var index = i;
                var queue = queues[index];
                var callback = _callbacks[queue];

                await Task.Factory
                    .StartNew(() => StartQueue(index, queue, callback.RoutingKey, callback.Callbacks), TaskCreationOptions.DenyChildAttach)
                    .ConfigureAwait(false);
            }
        }

        public async Task SendAsync<TMessage>(TMessage message)
        {
            var messageType = typeof(TMessage);
            var properties = new Dictionary<string, object>
            {
                {"sender", _processorName},
                {"message-type", messageType.FullName}
            };

            var body = _serializer.Serialize(message);
            var propertyBody = _serializer.Serialize(properties);

            using (var connection = new NpgsqlConnection(_options.ConnectionString))
            {
                await connection.OpenAsync().ConfigureAwait(false);
                try
                {
                    using (var transaction = connection.BeginTransaction(IsolationLevel.ReadCommitted))
                    {
                        try
                        {
                            var routingKey = $"{messageType}".ToLower();
                            var queues = await connection
                                .QueryAsync<Queue>($"SELECT queueid, name, routingkey FROM {_schemaName}.queues where routingkey = '{routingKey}'")
                                .ConfigureAwait(false);

                            foreach (var queue in queues)
                            {
                                _logger.LogDebug("Publishing message to queue {QueueName}", queue.Name);

                                await connection
                                    .ExecuteAsync($"INSERT INTO {_schemaName}.queuemessages ( QueueId, Body, Properties, State) VALUES ({queue.QueueId}, '{body}', '{propertyBody}', '{MessageState.Queued}');")
                                    .ConfigureAwait(false);
                            }

                            await transaction.CommitAsync().ConfigureAwait(false);
                        }
                        catch
                        {
                            await transaction.RollbackAsync().ConfigureAwait(false);
                            throw;
                        }
                    }
                }
                finally
                {
                    connection.Close();
                }
            }
        }

        public void Stop()
        {
            if (!_tokens.IsCancellationRequested)
            {
                _tokens.Cancel();
            }
        }

        public void Dispose()
        {
            Stop();
            _tokens.Dispose();
        }

        private async Task StartQueue(int threadNo, string queueName, string routingKey, List<Func<IBusMessage, Task>> callbacks)
        {
            queueName = queueName.ToLower();
            routingKey = routingKey.ToLower();

            await CreeateQueueIfNotExistsAsync(queueName, routingKey).ConfigureAwait(false);

            var queue = await FetchQueueAsync(queueName).ConfigureAwait(false);
            if (queue == null)
                throw new Exception($"Unable to fetch queue with name '{queueName}'");

            var stateProcessor = $"{_processorName}:{threadNo}".ToLower();

            var callbackCounter = 0;
            while (true)
            {
                if (_tokens.Token.IsCancellationRequested)
                    break;

                var callbackCounterInt = 0;
                try
                {
                    callbackCounterInt = await ProcessQueue(callbacks, stateProcessor, queue).ConfigureAwait(false);
                }
                catch (CallbackExecutionException e)
                {
                    _logger.LogError(e,
                        "Message execution callback failed! Message id: {ReceivedMessageId}. Callback number: {MessageCallbackNo}. Processor {StateProcessor}.",
                        e.MessageId, e.CallbackNo, stateProcessor);
                }
                catch (Exception e)
                {
                    _logger.LogError(e, "Queue processing failed! State processor: {StateProcessor}.", stateProcessor);
                }

                callbackCounter += callbackCounterInt;

                if (_tokens.Token.IsCancellationRequested)
                    break;

                if (callbackCounterInt == 0 && _options.SleepTime > TimeSpan.Zero)
                {
                    await Task.Delay(_options.SleepTime).ConfigureAwait(false);
                }
            }

            _logger.LogInformation("Finished queue {QueueName} loop. Executed {CallbackCount} callbacks.", queueName, callbackCounter);
        }

        private async Task<int> ProcessQueue(List<Func<IBusMessage, Task>> callbacks, string stateProcessor, Queue queue)
        {
            var callbackCounter = 0;

            using (var connection = new NpgsqlConnection(_options.ConnectionString))
            {
                await connection.OpenAsync().ConfigureAwait(false);

                try
                {
                    using (var transaction = connection.BeginTransaction(IsolationLevel.ReadCommitted))
                    {
                        try
                        {
                            var sql = $" UPDATE {_schemaName}.queuemessages" +
                                      $" SET state = '{MessageState.Processing}', Updated = clock_timestamp(), StateProcessor = '{stateProcessor}'" +
                                      $" WHERE queuemessageid = (" +
                                      $"    select queuemessageid from {_schemaName}.queuemessages WHERE QueueId = {queue.QueueId} AND State = '{MessageState.Queued}' ORDER BY created ASC LIMIT 1 FOR UPDATE SKIP LOCKED)" +
                                      $" RETURNING queuemessageid";

                            var queueMessageId = await connection.ExecuteScalarAsync<Guid?>(sql).ConfigureAwait(false);

                            if (queueMessageId.HasValue)
                            {
                                _logger.LogDebug("Received message id: {ReceivedMessageId}", queueMessageId);

                                var queueMessage = await FetchQueueMessageAsync(connection, queueMessageId.Value).ConfigureAwait(false);
                                if (queueMessage == null)
                                {
                                    _logger.LogWarning("Received message not loaded by id: {ReceivedMessageId}", queueMessageId);
                                    return 0;
                                }

                                // did we loaded wrong message?
                                if (queueMessage.StateProcessor != stateProcessor)
                                {
                                    _logger.LogWarning(
                                        "Received message id: '{ReceivedMessageId}' state processor does not match. Expected: {ExpectedStateProcessor}. Received: {ReceivedStateProcessor}.",
                                        queueMessageId, stateProcessor, queueMessage.StateProcessor);
                                    return 0;
                                }

                                var messageId = queueMessage.QueueMessageId.ToString();
                                var body = Encoding.UTF8.GetBytes(queueMessage.Body);
                                var properties = _serializer.Deserialize<IDictionary<string, object>>(queueMessage.Properties);
                                var busMessage = new PostgreSqlBusMessage(messageId, body, properties);

                                callbackCounter = await ProcessMessageCallbacks(connection, busMessage, callbacks, queueMessageId.Value).ConfigureAwait(false);
                            }

                            await transaction.CommitAsync().ConfigureAwait(false);
                        }
                        catch
                        {
                            await transaction.RollbackAsync().ConfigureAwait(false);
                            throw;
                        }
                    }
                }
                finally
                {
                    connection.Close();
                }
            }

            return callbackCounter;
        }

        private async Task<int> ProcessMessageCallbacks(NpgsqlConnection connection, PostgreSqlBusMessage busMessage, List<Func<IBusMessage, Task>> callbacks, Guid queueMessageId)
        {
            var timer = Stopwatch.StartNew();
            for (var i = 0; i < callbacks.Count; i++)
            {
                var callback = callbacks[i];
                try
                {
                    await callback(busMessage).ConfigureAwait(false);
                }
                catch (Exception e)
                {
                    await connection.ExecuteAsync(
                            $"UPDATE {_schemaName}.queuemessages " +
                            $"SET state = '{MessageState.Failed}', Updated = clock_timestamp(), StateDescription = '{e}' " +
                            $"WHERE queuemessageid = '{queueMessageId}'")
                        .ConfigureAwait(false);

                    throw new CallbackExecutionException(i, queueMessageId, e);
                }
            }

            timer.Stop();

            var stateDescription = $"Time spent={timer.Elapsed};";
            await connection.ExecuteAsync(
                    $"UPDATE {_schemaName}.queuemessages " +
                    $"SET state = '{MessageState.Completed}', Updated = clock_timestamp(), StateDescription = '{stateDescription}' " +
                    $"WHERE queuemessageid = '{queueMessageId}'")
                .ConfigureAwait(false);

            return callbacks.Count;
        }

        private async Task InitializeSchemaAsync()
        {
            const int retries = 10;
            for (int i = 0; i < retries; i++)
            {
                try
                {
                    using (var connection = new NpgsqlConnection(_options.ConnectionString))
                    {
                        await connection.OpenAsync().ConfigureAwait(false);

                        try
                        {
                            await connection.ExecuteAsync("CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\"").ConfigureAwait(false);
                            await connection.ExecuteAsync($"CREATE SCHEMA IF NOT EXISTS {_schemaName}").ConfigureAwait(false);
                            await connection.ExecuteAsync($"CREATE SEQUENCE IF NOT EXISTS {_schemaName}.SEQ_QueueId").ConfigureAwait(false);
                            await connection.ExecuteAsync($" CREATE TABLE IF NOT EXISTS {_schemaName}.queues (" +
                                                          $" QueueId INT NOT NULL CONSTRAINT PK_QueueId PRIMARY KEY DEFAULT nextval('{_schemaName}.SEQ_QueueId')," +
                                                          "  Name VARCHAR(1024) NOT NULL," +
                                                          "  RoutingKey VARCHAR(1024) NOT NULL," +
                                                          "  CONSTRAINT UC_Queues_Name UNIQUE (Name));")
                                .ConfigureAwait(false);
                            await connection.ExecuteAsync($" CREATE TABLE IF NOT EXISTS {_schemaName}.queuemessages (" +
                                                          "  QueueMessageId UUID NOT NULL CONSTRAINT PK_QueueMessageId PRIMARY KEY DEFAULT uuid_generate_v4()," +
                                                          "  QueueId INT NOT NULL," +
                                                          "  Body TEXT NOT NULL," +
                                                          "  Properties TEXT NOT NULL," +
                                                          "  State VARCHAR(10) NOT NULL," +
                                                          "  StateDescription TEXT NULL," +
                                                          "  StateProcessor VARCHAR(1024) NULL," +
                                                          "  Created TIMESTAMP NOT NULL DEFAULT clock_timestamp()," +
                                                          "  Updated TIMESTAMP NOT NULL DEFAULT clock_timestamp()," +
                                                          $" CONSTRAINT FK_QueueMessages_Queue FOREIGN KEY (QueueId) REFERENCES {_schemaName}.queues (QueueId) ON DELETE CASCADE);")
                                .ConfigureAwait(false);
                        }
                        finally
                        {
                            connection.Close();
                        }
                    }

                    break;
                }
                catch
                {
                    if (i + 1 == retries)
                        throw;
                    await Task.Delay(TimeSpan.FromSeconds(1)).ConfigureAwait(false);
                }
            }
        }

        private async Task CreeateQueueIfNotExistsAsync(string queueName, string routingKey)
        {
            const int retries = 10;
            for (int i = 0; i < retries; i++)
            {
                try
                {
                    using (var connection = new NpgsqlConnection(_options.ConnectionString))
                    {
                        await connection.OpenAsync().ConfigureAwait(false);

                        try
                        {
                            using (var transaction = connection.BeginTransaction(IsolationLevel.Serializable))
                            {
                                try
                                {
                                    var sql = $"INSERT INTO {_schemaName}.queues(Name, RoutingKey) VALUES('{queueName}', '{routingKey}') ON CONFLICT (Name) DO NOTHING"; 
                                    var affcetedRows = await connection
                                        .ExecuteAsync(sql)
                                        .ConfigureAwait(false);
                                    await transaction.CommitAsync().ConfigureAwait(false);
                                }
                                catch
                                {
                                    await transaction.RollbackAsync().ConfigureAwait(false);
                                    throw;
                                }
                            }
                        }
                        finally
                        {
                            connection.Close();
                        }
                    }
                }
                catch
                {
                    if (i + 1 == retries)
                        throw;
                    await Task.Delay(TimeSpan.FromSeconds(1)).ConfigureAwait(false);
                }
            }
        }

        private async Task<Queue> FetchQueueAsync(string queueName)
        {
            using (var connection = new NpgsqlConnection(_options.ConnectionString))
            {
                await connection.OpenAsync().ConfigureAwait(false);

                try
                {
                    var queues = await connection.QueryAsync<Queue>($"SELECT QueueId, Name FROM {_schemaName}.queues WHERE Name = '{queueName}'").ConfigureAwait(false);
                    return queues.FirstOrDefault();
                }
                finally
                {
                    connection.Close();
                }
            }
        }

        private async Task<QueueMessage> FetchQueueMessageAsync(IDbConnection connection, Guid queueMessageId)
        {
            var rows = await connection.QueryAsync<QueueMessage>(
                    $"SELECT queuemessageid, queueid, body, properties, state, statedescription, stateprocessor, created, updated " +
                    $"FROM {_schemaName}.queuemessages " +
                    $"WHERE queuemessageid = '{queueMessageId}'")
                .ConfigureAwait(false);

            return rows.FirstOrDefault();
        }
    }

/*
 * Queues
 *  QueueId
 *  Name: string(1024)
 *  
 * QueueMessages
 *  QueueMessageId
 *  QueueId
 *  Body
 *  Properties
 *  State [Queued, Processing, Completed, Failed]
 *  StateDescription
 *  Created
 *  Updated
 */
}
