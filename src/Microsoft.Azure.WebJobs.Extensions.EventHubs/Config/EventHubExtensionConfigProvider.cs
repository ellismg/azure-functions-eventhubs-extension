// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System;
using System.Text;
using System.Threading.Tasks;
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Processor;
using Azure.Messaging.EventHubs.Producer;
using Microsoft.Azure.WebJobs.Description;
using Microsoft.Azure.WebJobs.Host.Bindings;
using Microsoft.Azure.WebJobs.Host.Config;
using Microsoft.Azure.WebJobs.Host.Configuration;
using Microsoft.Azure.WebJobs.Logging;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Newtonsoft.Json;

namespace Microsoft.Azure.WebJobs.EventHubs
{
    [Extension("EventHubs", configurationSection: "EventHubs")]
    internal class EventHubExtensionConfigProvider : IExtensionConfigProvider
    {
        public IConfiguration _config;
        private readonly IOptions<EventHubOptions> _options;
        private readonly ILoggerFactory _loggerFactory;
        private readonly IConverterManager _converterManager;
        private readonly INameResolver _nameResolver;
        private readonly IWebJobsExtensionConfiguration<EventHubExtensionConfigProvider> _configuration;

        public EventHubExtensionConfigProvider(IConfiguration config, IOptions<EventHubOptions> options, ILoggerFactory loggerFactory,
            IConverterManager converterManager, INameResolver nameResolver, IWebJobsExtensionConfiguration<EventHubExtensionConfigProvider> configuration)
        {
            _config = config;
            _options = options;
            _loggerFactory = loggerFactory;
            _converterManager = converterManager;
            _nameResolver = nameResolver;
            _configuration = configuration;
        }

        public void Initialize(ExtensionConfigContext context)
        {
            if (context == null)
            {
                throw new ArgumentNullException(nameof(context));
            }
            
            _configuration.ConfigurationSection.Bind(_options);

            context
                .AddConverter<string, EventData>(ConvertString2EventData)
                .AddConverter<EventData, string>(ConvertEventData2String)
                .AddConverter<byte[], EventData>(ConvertBytes2EventData)
                .AddConverter<EventData, byte[]>(ConvertEventData2Bytes)
                .AddOpenConverter<OpenType.Poco, EventData>(ConvertPocoToEventData);

            // register our trigger binding provider
            var triggerBindingProvider = new EventHubTriggerAttributeBindingProvider(_config, _nameResolver, _converterManager, _options, _loggerFactory);
            context.AddBindingRule<EventHubTriggerAttribute>()
                .BindToTrigger(triggerBindingProvider);

            // register our binding provider
            context.AddBindingRule<EventHubAttribute>()
                .BindToCollector(BuildFromAttribute);

            context.AddBindingRule<EventHubAttribute>()
                .BindToInput(attribute =>
            {
                return _options.Value.GetEventHubProducerClient(attribute.EventHubName, attribute.Connection);
            });
        }

        private IAsyncCollector<EventData> BuildFromAttribute(EventHubAttribute attribute)
        {
            EventHubProducerClient client = _options.Value.GetEventHubProducerClient(attribute.EventHubName, attribute.Connection);
            return new EventHubAsyncCollector(client);
        }

        private static string ConvertEventData2String(EventData x)
            => Encoding.UTF8.GetString(ConvertEventData2Bytes(x));

        private static EventData ConvertBytes2EventData(byte[] input)
            => new EventData(input);

        private static byte[] ConvertEventData2Bytes(EventData input)
            => input.Body.ToArray();

        private static EventData ConvertString2EventData(string input)
            => ConvertBytes2EventData(Encoding.UTF8.GetBytes(input));

        private static Task<object> ConvertPocoToEventData(object arg, Attribute attrResolved, ValueBindingContext context)
        {
            return Task.FromResult<object>(ConvertString2EventData(JsonConvert.SerializeObject(arg)));
        }
    }
}
