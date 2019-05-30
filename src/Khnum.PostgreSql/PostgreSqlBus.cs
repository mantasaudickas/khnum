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
using Khnum.Contracts;
using Khnum.PostgreSql.Entities;
using Khnum.PostgreSql.Exceptions;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace Khnum.PostgreSql
{
    public class PostgreSqlBus: IBus
    {
        private static readonly string MachineName = Dns.GetHostName();

        private readonly ConcurrentDictionary<string, CallbackInfo> _callbacks = new ConcurrentDictionary<string, CallbackInfo>(StringComparer.OrdinalIgnoreCase);
        private readonly CancellationTokenSource _tokens = new CancellationTokenSource();
        private bool _finished = true;

        private readonly PostgreSqlRepository _db;

        private readonly PostgreSqlBusOptions _options;
        private readonly ISerializer _serializer;
        private readonly ILogger<PostgreSqlBus> _logger;
        private readonly string _processorName;

        public PostgreSqlBus(PostgreSqlBusOptions options, ISerializer serializer, ILogger<PostgreSqlBus> logger)
        {
            _db = new PostgreSqlRepository(options.Schema.ToLower(), logger);

            _options = options;
            _serializer = serializer;
            _logger = logger;
            _processorName = $"{MachineName}:{Process.GetCurrentProcess().Id}";
        }

        public async Task StartReceivers()
        {
            await InitializeSchemaAsync().ConfigureAwait(false);

            _finished = false;

            var queueList = _callbacks.Keys.Select(s => s.ToLower()).ToList();
            if (queueList.Count > 0)
            {
                var queues = new List<(Queue Queue, CallbackInfo Info)>();
                
                var consumerSet = new Dictionary<string, HashSet<string>>(StringComparer.OrdinalIgnoreCase);
                foreach (var queueName in queueList)
                {
                    var info = _callbacks[queueName];
                    var routingKey = info.RoutingKey.ToLower();

                    await CreateQueueIfNotExistsAsync(queueName, routingKey, info.ConsumerId).ConfigureAwait(false);

                    if (!consumerSet.TryGetValue(info.ConsumerId, out var queueSet))
                    {
                        queueSet = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                        consumerSet.Add(info.ConsumerId, queueSet);
                    }
                    queueSet.Add(queueName);

                    if (info.Callbacks.Count > 0)
                    {
                        var queue = await FetchQueueAsync(queueName).ConfigureAwait(false);
                        if (queue == null)
                            throw new PostgreSqlBusException($"Unable to fetch queue with name '{queueName}'");

                        queues.Add((queue, info));
                    }
                }

                // drop queues which belongs to my consumer ids but are not in my list
                foreach (var pair in consumerSet)
                {
                    var consumerId = pair.Key;
                    var queueSet = pair.Value;

                    var existingQueues = await FetchQueuesByServiceNameAsync(consumerId);
                    foreach (var existingQueue in existingQueues)
                    {
                        if (!queueSet.Contains(existingQueue.Name))
                        {
                            try
                            {
                                _logger.LogWarning("Dropping not used queue {QueueId}/{QueueName}", existingQueue.QueueId, existingQueue.Name);
                                await DropQueue(existingQueue.QueueId);
                            }
                            catch (Exception e)
                            {
                                // this should not break whole execution
                                _logger.LogError(e, "Unable to drop queue {QueueId}/{QueueName}", existingQueue.QueueId, existingQueue.Name);
                            }
                        }
                    }
                }

                if (queues.Count > 0)
                {
                    if (_options.RetentionPeriod > TimeSpan.Zero)
                    {
                        await Task.Factory
                            .StartNew(() => RetentionProcess(_options.RetentionPeriod, queues), TaskCreationOptions.DenyChildAttach)
                            .ConfigureAwait(false);
                    }
                    
                    await Task.Factory
                        .StartNew(() => StartQueueProcessor(queues), TaskCreationOptions.DenyChildAttach)
                        .ConfigureAwait(false);
                }
                else
                {
                    _finished = true;
                }
            }
        }

        private async Task RetentionProcess(TimeSpan retentionPeriod, List<(Queue Queue, CallbackInfo Info)> queues)
        {
            while (true)
            {
                var untilDateTime = DateTime.UtcNow.Subtract(retentionPeriod);
                _logger.LogInformation(
                    "Starting expired message retention. Messages which updated before {ExpirationDate} will be deleted.",
                    untilDateTime);

                foreach (var tuple in queues)
                {
                    try
                    {
                        await DeleteExpiredMessages(tuple.Queue.QueueId, untilDateTime).ConfigureAwait(false);
                    }
                    catch (Exception e)
                    {
                        _logger.LogWarning(e, $"Khnum message retention failed");
                    }
                }

                await Task.Delay(TimeSpan.FromHours(1), _tokens.Token);
            }
        }

        public void RegisterCallback(string consumerId, string queueName, string routingKey, Func<IBusMessage, Task> callback)
        {
            queueName = queueName.ToLower();
            routingKey = routingKey.ToLower();

            _logger.LogDebug("Registering callback for {QueueName} with routing key {RoutingKey}", queueName, routingKey);

            var info = _callbacks.GetOrAdd(queueName, new CallbackInfo
            {
                ConsumerId = consumerId,
                QueueName = queueName,
                RoutingKey = routingKey,
                Callbacks = new List<Func<IBusMessage, Task>>()
            });

            info.Callbacks.Add(callback);
        }

        public async Task PublishAsync<TMessage>(TMessage message)
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
                            var queues = await _db.FetchQueues(connection, routingKey).ConfigureAwait(false);

                            foreach (var queue in queues)
                            {
                                _logger.LogDebug("Publishing message to queue {QueueName}", queue.Name);

                                await _db.InsertNewMessage(connection, queue, body, propertyBody).ConfigureAwait(false);
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

            for (int i = 0; i < 30; i++)
            {
                if (!_finished)
                {
                    Thread.Sleep(TimeSpan.FromSeconds(2));
                }
                else
                {
                    break;
                }
            }

            _tokens.Dispose();
        }

        private async Task StartQueueProcessor(List<(Queue Queue, CallbackInfo Info)> queues)
        {
            try
            {
                var callbackCounter = 0;
                var stateProcessor = $"{_processorName}".ToLower();
                var queueIds = queues.Select(t => t.Queue.QueueId).ToArray();
                var callbackMap = queues.ToDictionary(t => t.Queue.QueueId, t => t.Info.Callbacks);

                while (true)
                {
                    if (_tokens.Token.IsCancellationRequested)
                        break;

                    var callbackCounterInt = 0;
                    using (var connection = new NpgsqlConnection(_options.ConnectionString))
                    {
                        await connection.OpenAsync().ConfigureAwait(false);

                        try
                        {
                            using (var transaction = connection.BeginTransaction(IsolationLevel.ReadCommitted))
                            {
                                callbackCounterInt = await TryReceive(connection, stateProcessor, queueIds, callbackMap)
                                    .ConfigureAwait(false);
                                await transaction.CommitAsync().ConfigureAwait(false);
                            }
                        }
                        catch (CallbackExecutionException e)
                        {
                            _logger.LogError(e,
                                "Message execution callback failed! Message id: {ReceivedMessageId}. Callback number: {MessageCallbackNo}. Processor {StateProcessor}.",
                                e.MessageId, e.CallbackNo, stateProcessor);
                        }
                        catch (Exception e)
                        {
                            _logger.LogError(e, "Queue processing failed! State processor: {StateProcessor}.",
                                stateProcessor);
                        }
                    }

                    callbackCounter += callbackCounterInt;

                    if (_tokens.Token.IsCancellationRequested)
                        break;

                    if (callbackCounterInt == 0 && _options.SleepTime > TimeSpan.Zero)
                    {
                        await Task.Delay(_options.SleepTime).ConfigureAwait(false);
                    }
                }

                _logger.LogInformation("Finished processing loop. Executed {CallbackCount} callbacks.",
                    callbackCounter);
            }
            catch (Exception e)
            {
                _logger.LogError(e, "StartQueueProcessor failed");
            }
            finally
            {
                _finished = true;
            }
        }

        private async Task<int> TryReceive(NpgsqlConnection connection, string stateProcessor, int[] queueIds, Dictionary<int, List<Func<IBusMessage, Task>>> callbackMap)
        {
            int callbackCounterInt = 0;
            var queueMessageId = await _db
                .FetchNextQueueMessageId(connection, stateProcessor, queueIds)
                .ConfigureAwait(false);

            if (queueMessageId.HasValue)
            {
                _logger.LogDebug("Received message id: {ReceivedMessageId}", queueMessageId);

                var queueMessage = await _db
                    .FetchQueueMessageAsync(connection, queueMessageId.Value)
                    .ConfigureAwait(false);

                if (queueMessage == null)
                {
                    _logger.LogWarning("Received message not loaded by id: {ReceivedMessageId}", queueMessageId);
                    return callbackCounterInt;
                }

                // did we loaded wrong message?
                if (queueMessage.StateProcessor != stateProcessor)
                {
                    _logger.LogWarning(
                        "Received message id: '{ReceivedMessageId}' state processor does not match. Expected: {ExpectedStateProcessor}. Received: {ReceivedStateProcessor}.",
                        queueMessageId, stateProcessor, queueMessage.StateProcessor);
                    return callbackCounterInt;
                }

                var messageId = queueMessage.QueueMessageId.ToString();
                var body = Encoding.UTF8.GetBytes(queueMessage.Body);
                var properties = _serializer.Deserialize<IDictionary<string, object>>(queueMessage.Properties);

                var callbacks = callbackMap[queueMessage.QueueId];

                var busMessage = new PostgreSqlBusMessage(messageId, body, properties);
                callbackCounterInt = await ProcessMessageCallbacks(connection, busMessage, callbacks, queueMessageId.Value).ConfigureAwait(false);
            }

            return callbackCounterInt;
        }

        private async Task<int> ProcessMessageCallbacks(NpgsqlConnection connection, PostgreSqlBusMessage message, List<Func<IBusMessage, Task>> callbacks, Guid queueMessageId)
        {
            var timer = Stopwatch.StartNew();

            for (var i = 0; i < callbacks.Count; i++)
            {
                var callback = callbacks[i];
                try
                {
                    await callback(message).ConfigureAwait(false);
                }
                catch (Exception e)
                {
                    await _db.UpdateMessageState(connection, queueMessageId, e.ToString(), MessageState.Failed).ConfigureAwait(false);
                    throw new CallbackExecutionException(i, queueMessageId, e);
                }
            }

            await _db.UpdateMessageState(connection, queueMessageId, $"Time spent={timer.Elapsed};", MessageState.Completed).ConfigureAwait(false);

            return callbacks.Count;
        }

        private async Task InitializeSchemaAsync()
        {
            const int retries = 10;
            for (int i = 0; i < retries; i++)
            {
                try
                {
                    _logger.LogInformation("Initializing database schema");

                    using (var connection = new NpgsqlConnection(_options.ConnectionString))
                    {
                        await connection.OpenAsync().ConfigureAwait(false);

                        try
                        {
                            await _db.SqlCreateOsspExtension(connection).ConfigureAwait(false);
                            await _db.SqlCreateSchema(connection).ConfigureAwait(false);
                            await _db.SqlCreateQueueSequence(connection).ConfigureAwait(false);
                            await _db.SqlCreateQueues(connection).ConfigureAwait(false);
                            await _db.SqlCreateQueueMessages(connection).ConfigureAwait(false);
                        }
                        finally
                        {
                            connection.Close();
                        }
                    }

                    break;
                }
                catch (Exception e)
                {
                    if (i + 1 == retries)
                        throw;
                    
                    _logger.LogWarning(e, "Schema initialization failed. Retry {Retry} out of {AllowedRetries}.", i + 1, retries);
                    await Task.Delay(TimeSpan.FromMilliseconds(500)).ConfigureAwait(false);
                }
            }
        }

        private async Task CreateQueueIfNotExistsAsync(string queueName, string routingKey, string serviceName)
        {
            const int retries = 10;
            for (int i = 0; i < retries; i++)
            {
                try
                {
                    _logger.LogInformation(
                        "Creating queue {QueueName} with routing key {RoutingKey} and service name {ServiceName}", 
                        queueName, routingKey, serviceName);

                    using (var connection = new NpgsqlConnection(_options.ConnectionString))
                    {
                        await connection.OpenAsync().ConfigureAwait(false);

                        try
                        {
                            using (var transaction = connection.BeginTransaction(IsolationLevel.Serializable))
                            {
                                try
                                {
                                    await _db.InsertQueue(connection, queueName, routingKey, serviceName).ConfigureAwait(false);
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

                    break;
                }
                catch (Exception e)
                {
                    if (i + 1 == retries)
                        throw;

                    _logger.LogWarning(e, "Queue creation failed. Retry {Retry} out of {AllowedRetries}.", i + 1, retries);
                    await Task.Delay(TimeSpan.FromMilliseconds(500)).ConfigureAwait(false);
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
                    var queues = await _db.FetchQueue(connection, queueName).ConfigureAwait(false);
                    return queues.FirstOrDefault();
                }
                finally
                {
                    connection.Close();
                }
            }
        }

        private async Task<IEnumerable<Queue>> FetchQueuesByServiceNameAsync(string serviceName)
        {
            try
            {
                using (var connection = new NpgsqlConnection(_options.ConnectionString))
                {
                    await connection.OpenAsync().ConfigureAwait(false);

                    try
                    {
                        var queues = await _db.FetchQueuesByServiceName(connection, serviceName).ConfigureAwait(false);
                        return queues;
                    }
                    finally
                    {
                        connection.Close();
                    }
                }
            }
            catch (Exception e)
            {
                _logger.LogError(e, "Unable to load queue list by service name {ServiceName}", serviceName);
                return new List<Queue>();
            }
        }

        private async Task DropQueue(int queueId)
        {
            using (var connection = new NpgsqlConnection(_options.ConnectionString))
            {
                await connection.OpenAsync().ConfigureAwait(false);

                try
                {
                    await _db.DropQueue(connection, queueId).ConfigureAwait(false);
                }
                finally
                {
                    connection.Close();
                }
            }
        }

        private async Task DeleteExpiredMessages(int queueId, DateTime untilDateTime)
        {
            using (var connection = new NpgsqlConnection(_options.ConnectionString))
            {
                await connection.OpenAsync().ConfigureAwait(false);

                try
                {
                    await _db.DeleteMessages(connection, queueId, untilDateTime).ConfigureAwait(false);
                }
                finally
                {
                    connection.Close();
                }
            }
        }
    }
}
