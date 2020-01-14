// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Producer;

namespace Microsoft.Azure.WebJobs.EventHubs
{
    /// <summary>
    /// Core object to send events to EventHub. 
    /// Any user parameter that sends EventHub events will eventually get bound to this object. 
    /// This will queue events and send in batches, also keeping under the 256kb event hub limit per batch. 
    /// </summary>
    internal class EventHubAsyncCollector : IAsyncCollector<EventData>
    {
        // TODO(matell): This is IAsyncDisposable. Do we need to push that disposability up?
        private readonly EventHubProducerClient _client;

        private readonly Dictionary<string, PartitionCollector> _partitions = new Dictionary<string, PartitionCollector>();
              
        private const int BatchSize = 100;

        // Suggested to use 240k instead of 256k to leave padding room for headers.
        private const int MaxByteSize = 240 * 1024; 
        
        /// <summary>
        /// Create a sender around the given client. 
        /// </summary>
        /// <param name="client"></param>
        public EventHubAsyncCollector(EventHubProducerClient client)
        {
            if (client == null)
            {
                throw new ArgumentNullException("client");
            }
            _client = client;
        }

        /// <summary>
        /// Add an event. 
        /// </summary>
        /// <param name="item">The event to add</param>
        /// <param name="cancellationToken">a cancellation token. </param>
        /// <returns></returns>
        public async Task AddAsync(EventData item, CancellationToken cancellationToken = default(CancellationToken))
        {
            if (item == null)
            {
                throw new ArgumentNullException("item");
            }

            // TODO(matell): I believe this proeprty will always be "null" on an EventData, since in the new SDK you
            // are unable to set this property when constructing an EventData object.  Instead, the Batch contains
            // the PartitionKey to use. See https://github.com/Azure/azure-functions-eventhubs-extension/issues/16
            // for a related issue.
            string key = item.PartitionKey ?? string.Empty;

            PartitionCollector partition;
            lock (_partitions)
            {
                if (!_partitions.TryGetValue(key, out partition))
                {
                    partition = new PartitionCollector(this);
                    _partitions[key] = partition;
                }
            }
            await partition.AddAsync(item, cancellationToken);
        }

        /// <summary>
        /// Asynchronously flush events that have been queued up via AddAsync.
        /// </summary>
        /// <param name="cancellationToken">a cancellation token</param>
        public async Task FlushAsync(CancellationToken cancellationToken = default(CancellationToken))
        {
            while (true)
            {
                PartitionCollector partition;
                lock (_partitions)
                {
                    if (_partitions.Count == 0)
                    {
                        return;
                    }
                    var kv = _partitions.First();
                    partition = kv.Value;
                    _partitions.Remove(kv.Key);
                }

                await partition.FlushAsync(cancellationToken);
            }
        }

        /// <summary>
        /// Send the batch of events. All items in the batch will have the same partition key. 
        /// </summary>
        /// <param name="batch">the set of events to send</param>
        protected virtual async Task SendBatchAsync(IEnumerable<EventData> batch)
        {
            using (EventDataBatch b = await _client.CreateBatchAsync())
            {
                foreach (EventData d in batch)
                {
                    // TODO(matell):  This feels fraglie, but relates to how we conmpute the size
                    // of the batch later.  I think in practice we can get this to not fail by correctly
                    // setting limits in PartitionCollector::AddAsync
                    if (!b.TryAdd(d))
                    {
                        throw new Exception("Failed to add data to batch.");
                    }
                }

                await _client.SendAsync(b);
            }
        }

        // A per-partition sender 
        private class PartitionCollector : IAsyncCollector<EventData>
        {
            private readonly EventHubAsyncCollector _parent;

            private List<EventData> _list = new List<EventData>();

            // total size of bytes in _list that we'll be sending in this batch. 
            private int _currentByteSize = 0;

            public PartitionCollector(EventHubAsyncCollector parent)
            {
                this._parent = parent;
            }

            /// <summary>
            /// Add an event. 
            /// </summary>
            /// <param name="item">The event to add</param>
            /// <param name="cancellationToken">a cancellation token. </param>
            /// <returns></returns>
            public async Task AddAsync(EventData item, CancellationToken cancellationToken = default(CancellationToken))
            {
                if (item == null)
                {
                    throw new ArgumentNullException("item");
                }

                while (true)
                {
                    lock (_list)
                    {
                        var size = item.Body.Length;

                        // TODO(matell): Is it correct to have this logic here?  Should we insted be determining the size of a batch from the service
                        // instead of a fixed value?
                        if (size > MaxByteSize)
                        {
                            // Single event is too large to add.
                            string msg = string.Format("Event is too large. Event is approximately {0}b and max size is {1}b", size, MaxByteSize);
                            throw new InvalidOperationException(msg);
                        }

                        bool flush = (_currentByteSize + size > MaxByteSize) || (_list.Count >= BatchSize);
                        if (!flush)
                        {
                            _list.Add(item);
                            _currentByteSize += size;
                            return;
                        }
                        // We should flush. 
                        // Release the lock, flush, and then loop around and try again. 
                    }

                    await this.FlushAsync(cancellationToken);
                }
            }

            /// <summary>
            /// Asynchronously flush events that have been queued up via AddAsync.
            /// </summary>
            /// <param name="cancellationToken">a cancellation token</param>
            public async Task FlushAsync(CancellationToken cancellationToken = default(CancellationToken))
            {
                EventData[] batch = null;
                lock (_list)
                {
                    batch = _list.ToArray();
                    _list.Clear();
                    _currentByteSize = 0;
                }

                if (batch.Length > 0)
                {
                    await _parent.SendBatchAsync(batch);
                }
            }
        }
    }
}