// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Consumer;
using Azure.Messaging.EventHubs.Processor;
using Azure.Storage.Blobs;
using Microsoft.Azure.WebJobs.EventHubs.Listeners;
using Microsoft.Azure.WebJobs.Host.Executors;
using Microsoft.Azure.WebJobs.Host.Listeners;
using Microsoft.Azure.WebJobs.Host.Scale;
using Microsoft.Extensions.Logging;

namespace Microsoft.Azure.WebJobs.EventHubs
{
    internal sealed class EventHubListener : IListener, IScaleMonitorProvider
    {
        private static readonly Dictionary<string, object> EmptyScope = new Dictionary<string, object>();
        private readonly string _functionId;
        private readonly string _eventHubName;
        private readonly string _consumerGroup;
        private readonly string _connectionString;
        private readonly string _storageConnectionString;
        private readonly ITriggeredFunctionExecutor _executor;
        private readonly EventProcessorClient _eventProcessorClient;
        private readonly bool _singleDispatch;
        private readonly EventHubOptions _options;
        private readonly ILogger _logger;
        private readonly ConcurrentDictionary<string, EventProcessor> _processors;
        private readonly ConcurrentDictionary<string, List<ProcessEventArgs>> _queuedEvents;
        private bool _started;

        private Lazy<EventHubsScaleMonitor> _scaleMonitor;

        public EventHubListener(
            string functionId,
            string eventHubName,
            string consumerGroup,
            string connectionString,
            string storageConnectionString,
            ITriggeredFunctionExecutor executor,
            EventProcessorClient eventProcessorClient,
            bool singleDispatch,
            EventHubOptions options,
            ILogger logger,
            BlobContainerClient blobContainer = null)
        {
            _functionId = functionId;
            _eventHubName = eventHubName;
            _consumerGroup = consumerGroup;
            _connectionString = connectionString;
            _storageConnectionString = storageConnectionString;
            _executor = executor;
            _eventProcessorClient = eventProcessorClient;
            _singleDispatch = singleDispatch;
            _options = options;
            _logger = logger;
            _scaleMonitor = new Lazy<EventHubsScaleMonitor>(() => new EventHubsScaleMonitor(_functionId, _eventHubName, _consumerGroup, _connectionString, _storageConnectionString, _logger, blobContainer));
            _processors = new ConcurrentDictionary<string, EventProcessor>();
            _queuedEvents = new ConcurrentDictionary<string, List<ProcessEventArgs>>();

            _eventProcessorClient.PartitionInitializingAsync += ParitionInitializingAsync;
            _eventProcessorClient.PartitionClosingAsync += PartitionClosingAsync;
            _eventProcessorClient.ProcessErrorAsync += ProcessErrorAsync;
            _eventProcessorClient.ProcessEventAsync += ProcessEventAsync;
        }

        void IListener.Cancel()
        {
            StopAsync(CancellationToken.None).Wait();
        }

        void IDisposable.Dispose()
        {
        }

        public Task ParitionInitializingAsync(PartitionInitializingEventArgs args)
        {
            EventProcessor processor = new EventProcessor(_options, _executor, _logger, _singleDispatch);
            _processors[args.PartitionId] = processor;
            return processor.ParitionInitializingAsync(args);            
        }

        public async Task PartitionClosingAsync(PartitionClosingEventArgs args)
        {
            EventProcessor procesor = _processors[args.PartitionId];
            await procesor.PartitionClosingAsync(args);
            _processors.TryRemove(args.PartitionId, out EventProcessor _);
        }

        public Task ProcessErrorAsync(ProcessErrorEventArgs args)
        {
            if (args.PartitionId == null)
            {
                try
                {
                    string message = $"EventProcessorClient error (Operation={args.Operation})";

                    var logLevel = GetLogLevel(args.Exception);
                    _logger?.Log(logLevel, 0, message, args.Exception, (s, ex) => message);
                }
                catch
                {
                    // best effort logging
                }

                return Task.CompletedTask;
            }

            return _processors[args.PartitionId].ProcessErrorAsync(args);
        }

        private static LogLevel GetLogLevel(Exception ex)
        {
            var ehex = ex as EventHubsException;

            if (!(ex is OperationCanceledException) && (ehex == null || !ehex.IsTransient))
            {
                // any non-transient exceptions or unknown exception types
                // we want to log as errors
                return LogLevel.Error;
            }
            else
            {
                // transient messaging errors we log as info so we have a record
                // of them, but we don't treat them as actual errors
                return LogLevel.Information;
            }
        }

        public Task ProcessEventAsync(ProcessEventArgs args)
        {
            var queuedPartitionEvents = _queuedEvents.GetOrAdd(args.Partition.PartitionId, new List<ProcessEventArgs>());

            if (args.HasEvent)
            {
                queuedPartitionEvents.Add(args);
            }

            if (queuedPartitionEvents.Count < _options.EventProcessorOptions.MaxBatchSize && args.HasEvent /* not due to timeout */)
            {
                // We enqueued the event to be handled later, nothing more for us to do now.
                return Task.CompletedTask;
            }
            
            // Timeout, but there's no events queued.  We can't call `EventProcessor.ProcessEventsAsync` because
            // we have no valid partition context to pass (note the documentation for `ProcessEventArgs.HasEvent`
            // implies that we would have no partition context.
            //
            // TODO(matell): What should we do about the above?
            if (!queuedPartitionEvents.Any())
            {
                return Task.CompletedTask;
            }

            ProcessEventArgs[] events = queuedPartitionEvents.ToArray();
            queuedPartitionEvents.Clear();
            return _processors[args.Partition.PartitionId].ProcessEventsAsync(events);
        }

        public async Task StartAsync(CancellationToken cancellationToken)
        {
            await _eventProcessorClient.StartProcessingAsync(cancellationToken);
            _started = true;
        }

        public async Task StopAsync(CancellationToken cancellationToken)
        {
            if (_started)
            {
                await _eventProcessorClient.StartProcessingAsync(cancellationToken);
            }
            _started = false;
        }

        public IScaleMonitor GetMonitor()
        {
            return _scaleMonitor.Value;
        }

        /// <summary>
        /// Wrapper for un-mockable checkpoint APIs to aid in unit testing
        /// </summary>
        internal interface ICheckpointer
        {
            Task CheckpointAsync(ProcessEventArgs args);
        }

        // We get a new instance each time Start() is called. 
        // We'll get a listener per partition - so they can potentialy run in parallel even on a single machine.
        internal class EventProcessor : IDisposable, ICheckpointer
        {
            private readonly ITriggeredFunctionExecutor _executor;
            private readonly bool _singleDispatch;
            private readonly ILogger _logger;
            private readonly CancellationTokenSource _cts = new CancellationTokenSource();
            private readonly ICheckpointer _checkpointer;
            private readonly int _batchCheckpointFrequency;
            private int _batchCounter = 0;
            private bool _disposed = false;

            public EventProcessor(EventHubOptions options, ITriggeredFunctionExecutor executor, ILogger logger, bool singleDispatch, ICheckpointer checkpointer = null)
            {
                _checkpointer = checkpointer ?? this;
                _executor = executor;
                _singleDispatch = singleDispatch;
                _batchCheckpointFrequency = options.BatchCheckpointFrequency;
                _logger = logger;
            }

            public Task PartitionClosingAsync(PartitionClosingEventArgs args)
            {
                // signal cancellation for any in progress executions 
                _cts.Cancel();

                return Task.CompletedTask;
            }

            public Task ParitionInitializingAsync(PartitionInitializingEventArgs args)
            {
                return Task.CompletedTask;
            }

            public Task ProcessErrorAsync(ProcessErrorEventArgs args)
            {
                // Note: Unlike the older version of EPH, we don't see exceptions like `ReceiverDisconnectedException` or `LeaseLostException`
                // during rebalancing. Instead, we would see a PartitionClosingAsync event when EventProcessor closes the partition.
                string errorDetails = $"Partition Id: '{args.PartitionId}', Operation: '{args.Operation}'";
                _logger.LogError(args.Exception, $"Error processing event from {errorDetails}");
                return Task.CompletedTask;
            }

            public async Task ProcessEventsAsync(ProcessEventArgs[] events)
            {
                var triggerInput = new EventHubTriggerInput
                {
                    Events = events.Select(x => x.Data).ToArray(),
                    PartitionContext = events.Last().Partition
                };

                if (_singleDispatch)
                {
                    // Single dispatch
                    int eventCount = triggerInput.Events.Length;
                    List<Task> invocationTasks = new List<Task>();
                    for (int i = 0; i < eventCount; i++)
                    {
                        if (_cts.IsCancellationRequested)
                        {
                            break;
                        }

                        var input = new TriggeredFunctionData
                        {
                            TriggerValue = triggerInput.GetSingleEventTriggerInput(i)
                        };

                        Task task = TryExecuteWithLoggingAsync(input, triggerInput.Events[i]);
                        invocationTasks.Add(task);
                    }

                    // Drain the whole batch before taking more work
                    if (invocationTasks.Count > 0)
                    {
                        await Task.WhenAll(invocationTasks);
                    }
                }
                else
                {
                    // Batch dispatch
                    var input = new TriggeredFunctionData
                    {
                        TriggerValue = triggerInput
                    };

                    using (_logger.BeginScope(GetLinksScope(triggerInput.Events)))
                    {
                        await _executor.TryExecuteAsync(input, _cts.Token);
                    }
                }

                // Checkpoint if we processed any events.
                // Don't checkpoint if no events. This can reset the sequence counter to 0.
                // Note: we intentionally checkpoint the batch regardless of function 
                // success/failure. EventHub doesn't support any sort "poison event" model,
                // so that is the responsibility of the user's function currently. E.g.
                // the function should have try/catch handling around all event processing
                // code, and capture/log/persist failed events, since they won't be retried.
                if (events.Any())
                {
                    await CheckpointAsync(events.Last());
                }
            }

            private async Task TryExecuteWithLoggingAsync(TriggeredFunctionData input, EventData message)
            {
                using (_logger.BeginScope(GetLinksScope(message)))
                {
                    await _executor.TryExecuteAsync(input, _cts.Token);
                }
            }

            private async Task CheckpointAsync(ProcessEventArgs args)
            {
                if (_batchCheckpointFrequency == 1)
                {
                    await _checkpointer.CheckpointAsync(args);
                }
                else
                {
                    // only checkpoint every N batches
                    if (++_batchCounter >= _batchCheckpointFrequency)
                    {
                        _batchCounter = 0;
                        await _checkpointer.CheckpointAsync(args);
                    }
                }
            }

            protected virtual void Dispose(bool disposing)
            {
                if (!_disposed)
                {
                    if (disposing)
                    {
                        _cts.Dispose();
                    }

                    _disposed = true;
                }
            }

            public void Dispose()
            {
                Dispose(true);
            }

            async Task ICheckpointer.CheckpointAsync(ProcessEventArgs args)
            {
                await args.UpdateCheckpointAsync();
            }

            private Dictionary<string, object> GetLinksScope(EventData message)
            {
                if (TryGetLinkedActivity(message, out var link))
                {
                    return new Dictionary<string, object> {["Links"] = new[] {link}};
                }

                return EmptyScope;
            }

            private Dictionary<string, object> GetLinksScope(EventData[] messages)
            {
                List<Activity> links = null;

                foreach (var message in messages)
                {
                    if (TryGetLinkedActivity(message, out var link))
                    {
                        if (links == null)
                        {
                            links = new List<Activity>(messages.Length);
                        }

                        links.Add(link);
                    }
                }

                if (links != null)
                {
                    return new Dictionary<string, object> {["Links"] = links};
                }

                return EmptyScope;
            }

            private bool TryGetLinkedActivity(EventData message, out Activity link)
            {
                link = null;
                return false;

                //TODO(matell): Figure out how this worked and re-enable it if we have support
                //link = null;

                //if (!message.Properties.ContainsKey("Diagnostic-Id"))
                //{
                //    return false;
                //}

                //link = message.ExtractActivity();
                //return true;
            }
        }
    }
}