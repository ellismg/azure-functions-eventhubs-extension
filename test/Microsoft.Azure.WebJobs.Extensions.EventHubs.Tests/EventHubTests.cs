// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System;
using System.Collections.Generic;
using System.Reflection;
using System.Text;
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Consumer;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Azure.WebJobs.Host.TestCommon;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Xunit;

namespace Microsoft.Azure.WebJobs.EventHubs.UnitTests
{
    public class EventHubTests
    {
        [Fact]
        public void GetStaticBindingContract_ReturnsExpectedValue()
        {
            var strategy = new EventHubTriggerBindingStrategy();
            var contract = strategy.GetBindingContract();

            Assert.Equal(7, contract.Count);
            Assert.Equal(typeof(PartitionContext), contract["PartitionContext"]);
            Assert.Equal(typeof(long), contract["Offset"]);
            Assert.Equal(typeof(long), contract["SequenceNumber"]);
            Assert.Equal(typeof(DateTimeOffset), contract["EnqueuedTime"]);
            Assert.Equal(typeof(IDictionary<string, object>), contract["Properties"]);
            Assert.Equal(typeof(IReadOnlyDictionary<string, object>), contract["SystemProperties"]);
        }

        [Fact]
        public void GetBindingContract_SingleDispatch_ReturnsExpectedValue()
        {
            var strategy = new EventHubTriggerBindingStrategy();
            var contract = strategy.GetBindingContract(true);

            Assert.Equal(7, contract.Count);
            Assert.Equal(typeof(PartitionContext), contract["PartitionContext"]);
            Assert.Equal(typeof(long), contract["Offset"]);
            Assert.Equal(typeof(long), contract["SequenceNumber"]);
            Assert.Equal(typeof(DateTimeOffset), contract["EnqueuedTime"]);
            Assert.Equal(typeof(IDictionary<string, object>), contract["Properties"]);
            Assert.Equal(typeof(IReadOnlyDictionary<string, object>), contract["SystemProperties"]);
        }

        [Fact]
        public void GetBindingContract_MultipleDispatch_ReturnsExpectedValue()
        {
            var strategy = new EventHubTriggerBindingStrategy();
            var contract = strategy.GetBindingContract(false);

            Assert.Equal(7, contract.Count);
            Assert.Equal(typeof(PartitionContext), contract["PartitionContext"]);
            Assert.Equal(typeof(string[]), contract["PartitionKeyArray"]);
            Assert.Equal(typeof(long[]), contract["OffsetArray"]);
            Assert.Equal(typeof(long[]), contract["SequenceNumberArray"]);
            Assert.Equal(typeof(DateTimeOffset[]), contract["EnqueuedTimeArray"]);
            Assert.Equal(typeof(IDictionary<string, object>[]), contract["PropertiesArray"]);
            Assert.Equal(typeof(IReadOnlyDictionary<string, object>[]), contract["SystemPropertiesArray"]);
        }

        [Fact]
        public void GetBindingData_SingleDispatch_ReturnsExpectedValue()
        {
            var evt = GetTestEventData(new byte[] { });

            var input = EventHubTriggerInput.New(evt);
            input.PartitionContext = GetPartitionContext();

            var strategy = new EventHubTriggerBindingStrategy();
            var bindingData = strategy.GetBindingData(input);

            Assert.Equal(7, bindingData.Count);
            Assert.Same(input.PartitionContext, bindingData["PartitionContext"]);
            Assert.Equal(evt.PartitionKey, bindingData["PartitionKey"]);
            Assert.Equal(evt.Offset, bindingData["Offset"]);
            Assert.Equal(evt.SequenceNumber, bindingData["SequenceNumber"]);
            Assert.Equal(evt.EnqueuedTime, bindingData["EnqueuedTime"]);
            Assert.Same(evt.Properties, bindingData["Properties"]);
            IDictionary<string, object> bindingDataSysProps = bindingData["SystemProperties"] as Dictionary<string, object>;
            Assert.NotNull(bindingDataSysProps);
            Assert.Equal(bindingDataSysProps["x-opt-partition-key"], bindingData["PartitionKey"]);
            Assert.Equal(bindingDataSysProps["x-opt-offset"], bindingData["Offset"]);
            Assert.Equal(bindingDataSysProps["x-opt-sequence-number"], bindingData["SequenceNumber"]);
            Assert.Equal(bindingDataSysProps["x-opt-enqueued-time"], bindingData["EnqueuedTime"]);
            Assert.Equal(bindingDataSysProps["iothub-connection-device-id"], "testDeviceId");
            Assert.Equal(bindingDataSysProps["iothub-enqueuedtime"], bindingData["EnqueuedTime"]);
        }

        private static EventData GetTestEventData(ReadOnlyMemory<byte> eventBody, string partitionKey = "TestKey")
        {
            var constructor = typeof(EventData).GetConstructor(
                BindingFlags.Instance | BindingFlags.NonPublic,
                null,
                new Type[] { typeof(ReadOnlyMemory<byte>), typeof(IDictionary<string, object>), typeof(IReadOnlyDictionary<string, object>), typeof(long), typeof(long), typeof(DateTimeOffset), typeof(string) },
                null);

            long testSequence = 4294967296;
            long testOffset = 12345;
            DateTimeOffset enquedTime = DateTimeOffset.MinValue;

            Dictionary<string, object> sysProps = new Dictionary<string, object>();
            sysProps["x-opt-partition-key"] = partitionKey;
            sysProps["x-opt-offset"] = testOffset;
            sysProps["x-opt-enqueued-time"] = enquedTime;
            sysProps["x-opt-sequence-number"] = testSequence;
            sysProps["iothub-connection-device-id"] = "testDeviceId";
            sysProps["iothub-enqueuedtime"] = enquedTime;

            return (EventData)constructor.Invoke(new object[] { eventBody, new Dictionary<string, object>(), sysProps, testSequence, testOffset, enquedTime, partitionKey });
        }

        [Fact]
        public void GetBindingData_MultipleDispatch_ReturnsExpectedValue()
        {
            var count = 0;
            var events = new EventData[3]
            {
                GetTestEventData(Encoding.UTF8.GetBytes("Event 1"), $"pk{count++}"),
                GetTestEventData(Encoding.UTF8.GetBytes("Event 2"), $"pk{count++}"),
                GetTestEventData(Encoding.UTF8.GetBytes("Event 3"), $"pk{count++}"),
            };

            var input = new EventHubTriggerInput
            {
                Events = events,
                PartitionContext = GetPartitionContext(),
            };
            var strategy = new EventHubTriggerBindingStrategy();
            var bindingData = strategy.GetBindingData(input);

            Assert.Equal(7, bindingData.Count);
            Assert.Same(input.PartitionContext, bindingData["PartitionContext"]);

            // verify an array was created for each binding data type
            Assert.Equal(events.Length, ((string[])bindingData["PartitionKeyArray"]).Length);
            Assert.Equal(events.Length, ((long[])bindingData["OffsetArray"]).Length);
            Assert.Equal(events.Length, ((long[])bindingData["SequenceNumberArray"]).Length);
            Assert.Equal(events.Length, ((DateTimeOffset[])bindingData["EnqueuedTimeArray"]).Length);
            Assert.Equal(events.Length, ((IDictionary<string, object>[])bindingData["PropertiesArray"]).Length);
            Assert.Equal(events.Length, ((IReadOnlyDictionary<string, object>[])bindingData["SystemPropertiesArray"]).Length);

            Assert.Equal(events[0].PartitionKey, ((string[])bindingData["PartitionKeyArray"])[0]);
            Assert.Equal(events[1].PartitionKey, ((string[])bindingData["PartitionKeyArray"])[1]);
            Assert.Equal(events[2].PartitionKey, ((string[])bindingData["PartitionKeyArray"])[2]);
        }

        [Fact]
        public void TriggerStrategy()
        {
            string data = "123";

            var strategy = new EventHubTriggerBindingStrategy();
            EventHubTriggerInput triggerInput = strategy.ConvertFromString(data);

            var contract = strategy.GetBindingData(triggerInput);

            EventData single = strategy.BindSingle(triggerInput, null);
            string body = Encoding.UTF8.GetString(single.Body.Span);

            Assert.Equal(data, body);
            Assert.Null(contract["PartitionContext"]);
            Assert.Null(contract["partitioncontext"]); // case insensitive
        }

        // Validate that if connection string has EntityPath, that takes precedence over the parameter.
        [Theory]
        [InlineData("k1", "Endpoint=sb://test89123-ns-x.servicebus.windows.net/;SharedAccessKeyName=ReceiveRule;SharedAccessKey=secretkey")]
        [InlineData("path2", "Endpoint=sb://test89123-ns-x.servicebus.windows.net/;SharedAccessKeyName=ReceiveRule;SharedAccessKey=secretkey;EntityPath=path2")]
        public void EntityPathInConnectionString(string expectedPathName, string connectionString)
        {
            EventHubOptions options = new EventHubOptions();

            // Test sender
            options.AddSender("k1", connectionString);
            var client = options.GetEventHubProducerClient("k1", null);
            Assert.Equal(expectedPathName, client.EventHubName);
        }

        // Validate that if connection string has EntityPath, that takes precedence over the parameter.
        [Theory]
        [InlineData("k1", "Endpoint=sb://test89123-ns-x.servicebus.windows.net/;SharedAccessKeyName=ReceiveRule;SharedAccessKey=secretkey")]
        [InlineData("path2", "Endpoint=sb://test89123-ns-x.servicebus.windows.net/;SharedAccessKeyName=ReceiveRule;SharedAccessKey=secretkey;EntityPath=path2")]
        public void GetEventHubClient_AddsConnection(string expectedPathName, string connectionString)
        {
            EventHubOptions options = new EventHubOptions();
            var client = options.GetEventHubProducerClient("k1", connectionString);
            Assert.Equal(expectedPathName, client.EventHubName);
        }

        [Theory]
        [InlineData("e", "n1", "n1/e/")]
        [InlineData("e--1", "host_.path.foo", "host_.path.foo/e--1/")]
        [InlineData("Ab", "Cd", "cd/ab/")]
        [InlineData("A=", "Cd", "cd/a:3D/")]
        [InlineData("A:", "Cd", "cd/a:3A/")]
        public void EventHubBlobPrefix(string eventHubName, string serviceBusNamespace, string expected)
        {
            string actual = EventHubOptions.GetBlobPrefix(eventHubName, serviceBusNamespace);
            Assert.Equal(expected, actual);
        }

        [Theory]
        [InlineData(1)]
        [InlineData(5)]
        [InlineData(200)]
        public void EventHubBatchCheckpointFrequency(int num)
        {
            var options = new EventHubOptions();
            options.BatchCheckpointFrequency = num;
            Assert.Equal(num, options.BatchCheckpointFrequency);
        }

        [Theory]
        [InlineData(-1)]
        [InlineData(0)]
        public void EventHubBatchCheckpointFrequency_Throws(int num)
        {
            var options = new EventHubOptions();
            Assert.Throws<InvalidOperationException>(() => options.BatchCheckpointFrequency = num);
        }

        [Fact]
        public void InitializeFromHostMetadata()
        {
            // TODO: It's tough to wire all this up without using a new host.
            IHost host = new HostBuilder()
                .ConfigureDefaultTestHost(builder =>
                {
                    builder.AddEventHubs();
                })
                .ConfigureAppConfiguration(c =>
                {
                    c.AddInMemoryCollection(new Dictionary<string, string>
                    {
                        { "AzureWebJobs:extensions:EventHubs:EventProcessorOptions:MaxBatchSize", "100" },
                        { "AzureWebJobs:extensions:EventHubs:BatchCheckpointFrequency", "5" },
                    });
                })
                .Build();

            // Force the ExtensionRegistryFactory to run, which will initialize the EventHubConfiguration.
            var extensionRegistry = host.Services.GetService<IExtensionRegistry>();
            var options = host.Services.GetService<IOptions<EventHubOptions>>().Value;

            Assert.Equal(5, options.BatchCheckpointFrequency);
        }

        internal static PartitionContext GetPartitionContext(string partitionId = "1")
        {
            var constructor = typeof(PartitionContext).GetConstructor(
                BindingFlags.NonPublic | BindingFlags.Instance,
                null,
                new Type[] { typeof(string) },
                null);

            return (PartitionContext)constructor.Invoke(new object[] { partitionId });
        }
    }
}
