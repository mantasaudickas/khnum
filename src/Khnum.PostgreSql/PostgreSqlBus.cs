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
        private readonly ConcurrentDictionary<string, CallbackInfo> _callbacks = new ConcurrentDictionary<string, CallbackInfo>();
        private readonly CancellationTokenSource _tokens = new CancellationTokenSource();

        private readonly PostgreSqlRepository _db;

        private readonly PostgreSqlBusOptions _options;
        private readonly IKhnumScopeFactory _scopeFactory;
        private readonly ISerializer _serializer;
        private readonly ILogger<PostgreSqlBus> _logger;
        private readonly string _processorName;

        public PostgreSqlBus(PostgreSqlBusOptions options, IKhnumScopeFactory scopeFactory, ISerializer serializer, ILogger<PostgreSqlBus> logger)
        {
            _db = new PostgreSqlRepository(options.Schema.ToLower(), logger);

            _options = options;
            _scopeFactory = scopeFactory;
            _serializer = serializer;
            _logger = logger;
            _processorName = $"{MachineName}:{Process.GetCurrentProcess().Id}";
        }

        public async Task StartReceivers()
        {
            await InitializeSchemaAsync().ConfigureAwait(false);

            var queues = _callbacks.Keys.ToList();
            for (int i = 0; i < queues.Count; ++i)
            {
                var index = i;
                var queueName = queues[index];
                var info = _callbacks[queueName];

                _logger.LogInformation(
                    "Starting queue {Index} processor for {QueueName} with routing key {RoutingKey} and {CallbackCount} callbacks.", 
                    index, queueName, info.RoutingKey, info.Callbacks.Count);

                await Task.Factory
                    .StartNew(() => StartQueueProcessor(index, queueName, info.RoutingKey, info.Callbacks), TaskCreationOptions.DenyChildAttach)
                    .ConfigureAwait(false);
            }
        }

        public void RegisterCallback(string queueName, string routingKey, Func<IBusMessage, Task> callback)
        {
            queueName = queueName.ToLower();
            routingKey = routingKey.ToLower();

            _logger.LogDebug("Registering callback for {QueueName} with routing key {RoutingKey}", queueName, routingKey);

            var info = _callbacks.GetOrAdd(queueName, new CallbackInfo
            {
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
            _tokens.Dispose();
        }

        private async Task StartQueueProcessor(int threadNo, string queueName, string routingKey, List<Func<IBusMessage, Task>> callbacks)
        {
            try
            {
                queueName = queueName.ToLower();
                routingKey = routingKey.ToLower();

                await CreeateQueueIfNotExistsAsync(queueName, routingKey).ConfigureAwait(false);

                var queue = await FetchQueueAsync(queueName).ConfigureAwait(false);
                if (queue == null)
                    throw new PostgreSqlBusException($"Unable to fetch queue with name '{queueName}'");

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
            catch (Exception e)
            {
                _logger.LogError(e, "StartQueueProcessor failed");
            }
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
                            var queueMessageId = await _db.FetchNextQueueMessageId(connection, queue, stateProcessor).ConfigureAwait(false);

                            if (queueMessageId.HasValue)
                            {
                                _logger.LogDebug("Received message id: {ReceivedMessageId}", queueMessageId);

                                var queueMessage = await _db.FetchQueueMessageAsync(connection, queueMessageId.Value).ConfigureAwait(false);
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

                                using (var scope = _scopeFactory.CreateScope())
                                {
                                    var busMessage = new PostgreSqlBusMessage(messageId, body, properties, scope.Services);
                                    callbackCounter = await ProcessMessageCallbacks(connection, busMessage, callbacks, queueMessageId.Value).ConfigureAwait(false);
                                }
                            }

                            await transaction.CommitAsync().ConfigureAwait(false);
                        }
                        catch (CallbackExecutionException)
                        {
                            // save message state
                            await transaction.CommitAsync().ConfigureAwait(false);
                            throw;
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

        private async Task CreeateQueueIfNotExistsAsync(string queueName, string routingKey)
        {
            const int retries = 10;
            for (int i = 0; i < retries; i++)
            {
                try
                {
                    _logger.LogInformation("Creating queue {QueueName} with routing key {RoutingKey}", queueName, routingKey);

                    using (var connection = new NpgsqlConnection(_options.ConnectionString))
                    {
                        await connection.OpenAsync().ConfigureAwait(false);

                        try
                        {
                            using (var transaction = connection.BeginTransaction(IsolationLevel.Serializable))
                            {
                                try
                                {
                                    await _db.InsertQueue(connection, queueName, routingKey).ConfigureAwait(false);
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
    }
}
