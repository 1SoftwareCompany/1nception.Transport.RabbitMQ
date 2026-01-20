using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using One.Inception.EventStore.Index;
using One.Inception.MessageProcessing;
using One.Inception.Migrations;
using One.Inception.Multitenancy;
using One.Inception.Transport.RabbitMQ.DedicatedQueues;
using RabbitMQ.Client;

namespace One.Inception.Transport.RabbitMQ.Startup;

public abstract class RabbitMqStartup<T> : IInceptionStartup
{
    private readonly BoundedContext boundedContext;
    private readonly ISubscriberCollection<T> subscriberCollection;
    private readonly IRabbitMqConnectionFactory connectionFactory;
    private readonly BoundedContextRabbitMqNamer bcRabbitMqNamer;
    private readonly ILogger<RabbitMqStartup<T>> logger;
    private readonly RabbitMqConsumerOptions consumerOptions;

    private TenantsOptions tenantsOptions;
    private bool isSystemQueue = false;

    private readonly string regularQueueName;

    public RabbitMqStartup(IOptionsMonitor<RabbitMqConsumerOptions> consumerOptions, IOptionsMonitor<BoundedContext> boundedContext, IOptionsMonitor<TenantsOptions> tenantsOptionsMonitor, ISubscriberCollection<T> subscriberCollection, IRabbitMqConnectionFactory connectionFactory, BoundedContextRabbitMqNamer bcRabbitMqNamer, ILogger<RabbitMqStartup<T>> logger)
    {
        this.tenantsOptions = tenantsOptionsMonitor.CurrentValue;
        this.boundedContext = boundedContext.CurrentValue;
        this.subscriberCollection = subscriberCollection;
        this.connectionFactory = connectionFactory;
        this.bcRabbitMqNamer = bcRabbitMqNamer;
        this.logger = logger;
        this.consumerOptions = consumerOptions.CurrentValue;

        var type = typeof(T);

        isSystemQueue = typeof(ISystemHandler).IsAssignableFrom(type);

        regularQueueName = bcRabbitMqNamer.Get_QueueName(type, consumerOptions.CurrentValue.FanoutMode);

        tenantsOptionsMonitor.OnChange(async newOptions =>
        {
            if (logger.IsEnabled(LogLevel.Debug))
                this.logger.LogDebug("Tenant options re-loaded with {@options}", newOptions);
            
            tenantsOptions = newOptions;

            using (IConnection connection = await connectionFactory.CreateConnectionAsync().ConfigureAwait(false))
            using (var channel = await connection.CreateChannelAsync().ConfigureAwait(false))
            {
                IEnumerable<ISubscriber> subscribersWithDedicatedQueues = subscriberCollection.Subscribers.SubscribersWithDedicatedQueuesOnly();

                foreach (var subscriber in subscribersWithDedicatedQueues)
                {
                    string specialQueueName = bcRabbitMqNamer.Get_QueueName(subscriber.HandlerType, this.consumerOptions.FanoutMode);
                    await RecoverModelAsync(specialQueueName, channel, [subscriber]).ConfigureAwait(false);
                }

                IEnumerable<ISubscriber> theRestOfTheSubscribers = subscriberCollection.Subscribers.Except(subscribersWithDedicatedQueues);
                await RecoverModelAsync(regularQueueName, channel, theRestOfTheSubscribers).ConfigureAwait(false);
            }
        });
    }

    public async Task BootstrapAsync()
    {
        using (var connection = await connectionFactory.CreateConnectionAsync().ConfigureAwait(false))
        using (var channel = await connection.CreateChannelAsync().ConfigureAwait(false))
        {
            IEnumerable<ISubscriber> subscribersWithDedicatedQueues = subscriberCollection.Subscribers.SubscribersWithDedicatedQueuesOnly();

            foreach (var subscriber in subscribersWithDedicatedQueues)
            {
                string specialQueueName = bcRabbitMqNamer.Get_QueueName(subscriber.HandlerType, consumerOptions.FanoutMode);
                await RecoverModelAsync(specialQueueName, channel, [subscriber]).ConfigureAwait(false);
            }

            IEnumerable<ISubscriber> theRestOfTheSubscribers = subscriberCollection.Subscribers.Except(subscribersWithDedicatedQueues);
            await RecoverModelAsync(regularQueueName, channel, theRestOfTheSubscribers).ConfigureAwait(false);
        }
    }

    private Dictionary<string, Dictionary<string, List<string>>> BuildEventToHandler(IEnumerable<ISubscriber> subscribers)
    {
        // exchangeName, dictionary<eventType,List<handlers>>
        var event2Handler = new Dictionary<string, Dictionary<string, List<string>>>();

        foreach (ISubscriber subscriber in subscribers)
        {
            foreach (Type msgType in subscriber.GetInvolvedMessageTypes().Where(mt => typeof(ISystemMessage).IsAssignableFrom(mt) == isSystemQueue))
            {
                string bc = msgType.GetBoundedContext(boundedContext.Name);
                string messageContractId = msgType.GetContractId();
                IEnumerable<string> exchangeNames = bcRabbitMqNamer.Get_BindTo_ExchangeNames(msgType);

                foreach (string exchangeName in exchangeNames)
                {
                    Dictionary<string, List<string>> message2Handlers;
                    if (event2Handler.TryGetValue(exchangeName, out message2Handlers) == false)
                    {
                        message2Handlers = new Dictionary<string, List<string>>();
                        event2Handler.Add(exchangeName, message2Handlers);
                    }

                    List<string> handlers;
                    if (message2Handlers.TryGetValue(messageContractId, out handlers) == false)
                    {
                        handlers = new List<string>();
                        message2Handlers.Add(messageContractId, handlers);
                    }

                    handlers.Add(subscriber.Id);
                }
            }
        }

        return event2Handler;
    }

    private Dictionary<string, object> BuildQueueRoutingHeaders(IEnumerable<ISubscriber> subscribers)
    {
        var routingHeaders = new Dictionary<string, object>();

        foreach (var subscriber in subscribers)
        {
            foreach (var msgType in subscriber.GetInvolvedMessageTypes().Where(mt => typeof(ISystemMessage).IsAssignableFrom(mt) == isSystemQueue))
            {
                string bc = msgType.GetBoundedContext(boundedContext.Name);

                string messageContractId = msgType.GetContractId();
                if (routingHeaders.ContainsKey(messageContractId) == false)
                    routingHeaders.Add(messageContractId, bc);

                string handlerHeader = $"{messageContractId}@{subscriber.Id}";
                if (routingHeaders.ContainsKey(handlerHeader) == false)
                    routingHeaders.Add(handlerHeader, bc);
            }
        }

        return routingHeaders;
    }

    private async Task RecoverModelAsync(string queueName, IChannel channel, IEnumerable<ISubscriber> subscribers)
    {
        var messageTypes = subscribers.SelectMany(x => x.GetInvolvedMessageTypes()).Where(mt => typeof(ISystemMessage).IsAssignableFrom(mt) == isSystemQueue).Distinct().ToList();

        var publishToExchangeGroups = messageTypes
            .SelectMany(mt => bcRabbitMqNamer.Get_ExchangeNames_To_Declare(mt).Select(x => new { Exchange = x, MessageType = mt }))
            .GroupBy(x => x.Exchange)
            .Distinct()
            .ToList();

        foreach (var publishExchangeGroup in publishToExchangeGroups)
        {
            await channel.ExchangeDeclareAsync(publishExchangeGroup.Key, PipelineType.Headers.ToString(), true).ConfigureAwait(false);
        }

        Dictionary<string, Dictionary<string, List<string>>> event2Handler = BuildEventToHandler(subscribers);

        Dictionary<string, object> routingHeaders = BuildQueueRoutingHeaders(subscribers);
        await channel.QueueDeclareAsync(queueName, true, false, false, null).ConfigureAwait(false);

        var bindToExchangeGroups = messageTypes
            .SelectMany(mt => bcRabbitMqNamer.Get_BindTo_ExchangeNames(mt).Select(x => new { Exchange = x, MessageType = mt }))
            .GroupBy(x => x.Exchange)
            .Distinct()
            .ToList();

        bool thereIsAScheduledQueue = false;
        string scheduledQueue = string.Empty;

        bool isProcessManagerQueue = typeof(T).Name.Equals(typeof(IProcessManager).Name) || typeof(T).Name.Equals(typeof(ISystemProcessManager).Name);
        if (isProcessManagerQueue)
        {
            bool hasOneExchangeGroup = bindToExchangeGroups.Count == 1;
            if (hasOneExchangeGroup)
            {
                string targetExchangeAfterTtlExpires = bindToExchangeGroups[0].Key;
                var arguments = new Dictionary<string, object>()
                {
                    { "x-dead-letter-exchange", targetExchangeAfterTtlExpires}
                };

                scheduledQueue = $"{queueName}.Scheduled";
                await channel.QueueDeclareAsync(scheduledQueue, true, false, false, arguments).ConfigureAwait(false);

                thereIsAScheduledQueue = true;
            }
            else if (bindToExchangeGroups.Count > 1)
            {
                throw new Exception($"There are more than one exchanges defined for {typeof(T).Name}. RabbitMQ does not allow this functionality and you need to fix one or more of the following subscribers:{Environment.NewLine}{string.Join(Environment.NewLine, subscribers.Select(sub => sub.Id))}");
            }
        }

        bool isTriggerQueue = typeof(T).Name.Equals(typeof(ITrigger).Name);
        if (isTriggerQueue && bindToExchangeGroups.Count > 0)
        {
            if (bindToExchangeGroups.Count > 1)
            {
                logger.LogWarning("There are more than one exchanges defined for {handlerType}. RabbitMQ does not allow this functionality. We will pick first exchange.", typeof(T).Name);
            }

            string targetExchangeAfterTtlExpires = bindToExchangeGroups[0].Key;
            var arguments = new Dictionary<string, object>()
            {
                { "x-dead-letter-exchange", targetExchangeAfterTtlExpires}
            };

            scheduledQueue = $"{queueName}.Scheduled";
            await channel.QueueDeclareAsync(scheduledQueue, true, false, false, arguments).ConfigureAwait(false);

            thereIsAScheduledQueue = true;
        }

        bool isIEventStoreIndex = typeof(T).Name.Equals(typeof(IEventStoreIndex).Name);
        foreach (var exchangeGroup in bindToExchangeGroups)
        {
            // Standard exchange
            string standardExchangeName = exchangeGroup.Key;
            await channel.ExchangeDeclareAsync(standardExchangeName, PipelineType.Headers.ToString(), true, false, null).ConfigureAwait(false);

            var bindHeaders = new Dictionary<string, object>();

            foreach (Type msgType in exchangeGroup.Select(x => x.MessageType))
            {
                string contractId = msgType.GetContractId();
                string bc = msgType.GetBoundedContext(boundedContext.Name);

                if (bc.Equals(boundedContext.Name, StringComparison.OrdinalIgnoreCase) == false && isSystemQueue)
                    throw new Exception($"The message {msgType.Name} has a bounded context {bc} which is different than the configured {boundedContext.Name}.");

                List<string> handlers = event2Handler[standardExchangeName][contractId];
                if (isTriggerQueue == false)
                {
                    if (isIEventStoreIndex && (typeof(IPublicEvent)).IsAssignableFrom(msgType)) // public event that needs to be handled in index, so we prefix the tenant
                    {
                        BuildHeadersForMessageTypeOutsideCurrentBC(contractId, bc, bindHeaders, handlers);
                    }
                    else
                    {
                        if (bc.Equals(boundedContext.Name, StringComparison.OrdinalIgnoreCase) == false)
                        {
                            BuildHeadersForMessageTypeOutsideCurrentBC(contractId, bc, bindHeaders, handlers);
                        }
                        else
                        {
                            BuildHeadersForMessageTypeForCurrentBC(contractId, bc, bindHeaders, handlers);
                        }
                    }
                }
                else // TRIGGER
                {
                    BuildHeadersForMessageTypeForCurrentBC(contractId, bc, bindHeaders, handlers); // here we put both because we can have signals in the same BC and ALSO between diff systems
                    BuildHeadersForMessageTypeOutsideCurrentBC(contractId, bc, bindHeaders, handlers);
                }

            }

            foreach (var header in bindHeaders)
            {
                await channel.QueueBindAsync(queueName, standardExchangeName, string.Empty, new Dictionary<string, object> { { header.Key, header.Value } }).ConfigureAwait(false);
            }

            if (thereIsAScheduledQueue)
            {
                foreach (var header in bindHeaders)
                {
                    string deadLetterExchangeName = $"{standardExchangeName}.Delayer";
                    await channel.ExchangeDeclareAsync(deadLetterExchangeName, ExchangeType.Headers, true, false).ConfigureAwait(false);
                    await channel.QueueBindAsync(scheduledQueue, deadLetterExchangeName, string.Empty, new Dictionary<string, object> { { header.Key, header.Value } }).ConfigureAwait(false);
                }
            }
        }
    }

    private void BuildHeadersForMessageTypeOutsideCurrentBC(string messageContractId, string currentBC, Dictionary<string, object> headersRef, List<string> handlers)
    {
        headersRef.TryAdd(messageContractId, currentBC);

        foreach (string tenant in tenantsOptions.Tenants)
        {
            string contractIdWithTenant = $"{messageContractId}@{tenant}";
            headersRef.Add(contractIdWithTenant, currentBC);

            foreach (var handler in handlers)
            {
                string key = $"{messageContractId}@{handler}@{tenant}";
                headersRef.Add(key, currentBC);
            }
        }
    }

    private void BuildHeadersForMessageTypeForCurrentBC(string messageContractId, string currentBC, Dictionary<string, object> headersRef, List<string> handlers)
    {
        headersRef.TryAdd(messageContractId, currentBC);

        foreach (var handler in handlers)
        {
            headersRef.Add($"{messageContractId}@{handler}", currentBC);
        }
    }
}

[InceptionStartup(Bootstraps.Configuration)]
public class AppService_Startup : RabbitMqStartup<IApplicationService>
{
    public AppService_Startup(IOptionsMonitor<TenantsOptions> tenantsOptions, IOptionsMonitor<RabbitMqConsumerOptions> consumerOptions, IOptionsMonitor<BoundedContext> boundedContext, ISubscriberCollection<IApplicationService> subscriberCollection, IRabbitMqConnectionFactory connectionFactory, BoundedContextRabbitMqNamer bcRabbitMqNamer, ILogger<AppService_Startup> logger) : base(consumerOptions, boundedContext, tenantsOptions, subscriberCollection, connectionFactory, bcRabbitMqNamer, logger) { }
}

[InceptionStartup(Bootstraps.Configuration)]
public class SystemEventStoreIndex_Startup : RabbitMqStartup<ISystemEventStoreIndex>
{
    public SystemEventStoreIndex_Startup(IOptionsMonitor<TenantsOptions> tenantsOptions, IOptionsMonitor<RabbitMqConsumerOptions> consumerOptions, IOptionsMonitor<BoundedContext> boundedContext, ISubscriberCollection<ISystemEventStoreIndex> subscriberCollection, IRabbitMqConnectionFactory connectionFactory, BoundedContextRabbitMqNamer bcRabbitMqNamer, ILogger<SystemEventStoreIndex_Startup> logger) : base(consumerOptions, boundedContext, tenantsOptions, subscriberCollection, connectionFactory, bcRabbitMqNamer, logger) { }
}

[InceptionStartup(Bootstraps.Configuration)]
public class EventStoreIndex_Startup : RabbitMqStartup<IEventStoreIndex>
{
    public EventStoreIndex_Startup(IOptionsMonitor<TenantsOptions> tenantsOptions, IOptionsMonitor<RabbitMqConsumerOptions> consumerOptions, IOptionsMonitor<BoundedContext> boundedContext, ISubscriberCollection<IEventStoreIndex> subscriberCollection, IRabbitMqConnectionFactory connectionFactory, BoundedContextRabbitMqNamer bcRabbitMqNamer, ILogger<EventStoreIndex_Startup> logger) : base(consumerOptions, boundedContext, tenantsOptions, subscriberCollection, connectionFactory, bcRabbitMqNamer, logger) { }
}

[InceptionStartup(Bootstraps.Configuration)]
public class Projection_Startup : RabbitMqStartup<IProjection>
{
    public Projection_Startup(IOptionsMonitor<TenantsOptions> tenantsOptions, IOptionsMonitor<RabbitMqConsumerOptions> consumerOptions, IOptionsMonitor<BoundedContext> boundedContext, ISubscriberCollection<IProjection> subscriberCollection, IRabbitMqConnectionFactory connectionFactory, BoundedContextRabbitMqNamer bcRabbitMqNamer, ILogger<Projection_Startup> logger) : base(consumerOptions, boundedContext, tenantsOptions, subscriberCollection, connectionFactory, bcRabbitMqNamer, logger) { }
}

[InceptionStartup(Bootstraps.Configuration)]
public class Port_Startup : RabbitMqStartup<IPort>
{
    public Port_Startup(IOptionsMonitor<TenantsOptions> tenantsOptions, IOptionsMonitor<RabbitMqConsumerOptions> consumerOptions, IOptionsMonitor<BoundedContext> boundedContext, ISubscriberCollection<IPort> subscriberCollection, IRabbitMqConnectionFactory connectionFactory, BoundedContextRabbitMqNamer bcRabbitMqNamer, ILogger<Port_Startup> logger) : base(consumerOptions, boundedContext, tenantsOptions, subscriberCollection, connectionFactory, bcRabbitMqNamer, logger) { }
}

[InceptionStartup(Bootstraps.Configuration)]
public class ProcessManager_Startup : RabbitMqStartup<IProcessManager>
{
    public ProcessManager_Startup(IOptionsMonitor<TenantsOptions> tenantsOptions, IOptionsMonitor<RabbitMqConsumerOptions> consumerOptions, IOptionsMonitor<BoundedContext> boundedContext, ISubscriberCollection<IProcessManager> subscriberCollection, IRabbitMqConnectionFactory connectionFactory, BoundedContextRabbitMqNamer bcRabbitMqNamer, ILogger<ProcessManager_Startup> logger) : base(consumerOptions, boundedContext, tenantsOptions, subscriberCollection, connectionFactory, bcRabbitMqNamer, logger) { }
}

[InceptionStartup(Bootstraps.Configuration)]
public class Gateway_Startup : RabbitMqStartup<IGateway>
{
    public Gateway_Startup(IOptionsMonitor<TenantsOptions> tenantsOptions, IOptionsMonitor<RabbitMqConsumerOptions> consumerOptions, IOptionsMonitor<BoundedContext> boundedContext, ISubscriberCollection<IGateway> subscriberCollection, IRabbitMqConnectionFactory connectionFactory, BoundedContextRabbitMqNamer bcRabbitMqNamer, ILogger<Gateway_Startup> logger) : base(consumerOptions, boundedContext, tenantsOptions, subscriberCollection, connectionFactory, bcRabbitMqNamer, logger) { }
}

[InceptionStartup(Bootstraps.Configuration)]
public class TriggerPrivate_Startup : RabbitMqStartup<ITrigger>
{
    public TriggerPrivate_Startup(IOptionsMonitor<TenantsOptions> tenantsOptions, IOptionsMonitor<RabbitMqConsumerOptions> consumerOptions, IOptionsMonitor<BoundedContext> boundedContext, ISubscriberCollection<ITrigger> subscriberCollection, IRabbitMqConnectionFactory connectionFactory, BoundedContextRabbitMqNamer bcRabbitMqNamer, ILogger<TriggerPrivate_Startup> logger) : base(consumerOptions, boundedContext, tenantsOptions, subscriberCollection, connectionFactory, bcRabbitMqNamer, logger) { }
}

[InceptionStartup(Bootstraps.Configuration)]
public class SystemAppService_Startup : RabbitMqStartup<ISystemAppService>
{
    public SystemAppService_Startup(IOptionsMonitor<TenantsOptions> tenantsOptions, IOptionsMonitor<RabbitMqConsumerOptions> consumerOptions, IOptionsMonitor<BoundedContext> boundedContext, ISubscriberCollection<ISystemAppService> subscriberCollection, IRabbitMqConnectionFactory connectionFactory, BoundedContextRabbitMqNamer bcRabbitMqNamer, ILogger<SystemAppService_Startup> logger) : base(consumerOptions, boundedContext, tenantsOptions, subscriberCollection, connectionFactory, bcRabbitMqNamer, logger) { }
}

[InceptionStartup(Bootstraps.Configuration)]
public class SystemProcessManager_Startup : RabbitMqStartup<ISystemProcessManager>
{
    public SystemProcessManager_Startup(IOptionsMonitor<TenantsOptions> tenantsOptions, IOptionsMonitor<RabbitMqConsumerOptions> consumerOptions, IOptionsMonitor<BoundedContext> boundedContext, ISubscriberCollection<ISystemProcessManager> subscriberCollection, IRabbitMqConnectionFactory connectionFactory, BoundedContextRabbitMqNamer bcRabbitMqNamer, ILogger<SystemProcessManager_Startup> logger) : base(consumerOptions, boundedContext, tenantsOptions, subscriberCollection, connectionFactory, bcRabbitMqNamer, logger) { }
}

[InceptionStartup(Bootstraps.Configuration)]
public class SystemPort_Startup : RabbitMqStartup<ISystemPort>
{
    public SystemPort_Startup(IOptionsMonitor<TenantsOptions> tenantsOptions, IOptionsMonitor<RabbitMqConsumerOptions> consumerOptions, IOptionsMonitor<BoundedContext> boundedContext, ISubscriberCollection<ISystemPort> subscriberCollection, IRabbitMqConnectionFactory connectionFactory, BoundedContextRabbitMqNamer bcRabbitMqNamer, ILogger<SystemPort_Startup> logger) : base(consumerOptions, boundedContext, tenantsOptions, subscriberCollection, connectionFactory, bcRabbitMqNamer, logger) { }
}

[InceptionStartup(Bootstraps.Configuration)]
public class SystemTrigger_Startup : RabbitMqStartup<ISystemTrigger>
{
    public SystemTrigger_Startup(IOptionsMonitor<TenantsOptions> tenantsOptions, IOptionsMonitor<RabbitMqConsumerOptions> consumerOptions, IOptionsMonitor<BoundedContext> boundedContext, ISubscriberCollection<ISystemTrigger> subscriberCollection, IRabbitMqConnectionFactory connectionFactory, BoundedContextRabbitMqNamer bcRabbitMqNamer, ILogger<SystemTrigger_Startup> logger) : base(consumerOptions, boundedContext, tenantsOptions, subscriberCollection, connectionFactory, bcRabbitMqNamer, logger) { }
}

[InceptionStartup(Bootstraps.Configuration)]
public class SystemProjection_Startup : RabbitMqStartup<ISystemProjection>
{
    public SystemProjection_Startup(IOptionsMonitor<TenantsOptions> tenantsOptions, IOptionsMonitor<RabbitMqConsumerOptions> consumerOptions, IOptionsMonitor<BoundedContext> boundedContext, ISubscriberCollection<ISystemProjection> subscriberCollection, IRabbitMqConnectionFactory connectionFactory, BoundedContextRabbitMqNamer bcRabbitMqNamer, ILogger<SystemProjection_Startup> logger) : base(consumerOptions, boundedContext, tenantsOptions, subscriberCollection, connectionFactory, bcRabbitMqNamer, logger) { }
}

[InceptionStartup(Bootstraps.Configuration)]
public class MigrationHandler_Startup : RabbitMqStartup<IMigrationHandler>
{
    public MigrationHandler_Startup(IOptionsMonitor<TenantsOptions> tenantsOptions, IOptionsMonitor<RabbitMqConsumerOptions> consumerOptions, IOptionsMonitor<BoundedContext> boundedContext, ISubscriberCollection<IMigrationHandler> subscriberCollection, IRabbitMqConnectionFactory connectionFactory, BoundedContextRabbitMqNamer bcRabbitMqNamer, ILogger<MigrationHandler_Startup> logger) : base(consumerOptions, boundedContext, tenantsOptions, subscriberCollection, connectionFactory, bcRabbitMqNamer, logger) { }
}
