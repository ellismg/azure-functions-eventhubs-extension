// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using Microsoft.Azure.EventHubs;
using Microsoft.Azure.EventHubs.Processor;
using System.Collections.Generic;

namespace Microsoft.Azure.WebJobs.EventHubs
{
    // The core object we get when an EventHub is triggered. 
    // This gets converted to the user type (EventData, string, poco, etc) 
    internal sealed class EventHubTriggerInput      
    {        
        // If != -1, then only process a single event in this batch. 
        private int _selector = -1;

        internal EventData[] Events { get; set; }

        internal PartitionContext PartitionContext { get; set; }

        public bool IsSingleDispatch
        {
            get
            {
                return _selector != -1;
            }
        }

        public static EventHubTriggerInput New(EventData eventData)
        {
            return new EventHubTriggerInput
            {
                PartitionContext = null,
                Events = new EventData[]
                {
                      eventData
                },
                _selector = 0,
            };
        }

        public EventHubTriggerInput GetSingleEventTriggerInput(int idx)
        {
            return new EventHubTriggerInput
            {
                Events = this.Events,
                PartitionContext = this.PartitionContext,
                _selector = idx
            };
        }

        public EventData GetSingleEventData()
        {
            return this.Events[this._selector];
        }

        public Dictionary<string, string> GetTriggerDetails(PartitionContext context)
        {
            if (IsSingleDispatch)
            {
                return new Dictionary<string, string>()
                {
                    { "PartionId", context.PartitionId },
                    { "Offset", string.Join(",", Events[0].SystemProperties?.Offset) },
                    { "EnqueueTimeUtc", string.Join(",", Events[0].SystemProperties?.EnqueuedTimeUtc.ToString("o")) },
                    { "PartitionKey", string.Join(",", Events[0].SystemProperties?.PartitionKey) },
                    { "SequenceNumber", string.Join(",", Events[0].SystemProperties?.SequenceNumber.ToString()) }
                };
            }
            else
            {
                string[] offsetArray = new string[Events.Length];
                string[] enqueueTimeUtcArray = new string[Events.Length];
                string[] partitionKeyArray = new string[Events.Length];
                string[] sequenceNumberArray = new string[Events.Length];

                for (int i = 0; i < Events.Length; i++)
                {
                    offsetArray[i] = Events[i].SystemProperties?.Offset;
                    enqueueTimeUtcArray[i] = Events[i].SystemProperties?.EnqueuedTimeUtc.ToString("o");
                    partitionKeyArray[i] = Events[i].SystemProperties?.PartitionKey;
                    sequenceNumberArray[i] = Events[i].SystemProperties?.SequenceNumber.ToString();
                }

                return new Dictionary<string, string>()
                {
                    { "PartionId", context.PartitionId },
                    { "OffsetArray", string.Join(",", offsetArray) },
                    { "EnqueueTimeUtcArray", string.Join(",", enqueueTimeUtcArray) },
                    { "PartitionKeyArray", string.Join(",", partitionKeyArray) },
                    { "SequenceNumberArray", string.Join(",", sequenceNumberArray) }
                };
            }
        }
    }
}