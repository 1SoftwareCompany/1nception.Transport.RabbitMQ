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
    private readonly QueueBindingArgumentsFactory queueBindingArgumentsFactory;

    private TenantsOptions tenantsOptions;
    private bool isSystemQueue = false;

    private readonly string regularQueueName;

    public RabbitMqStartup(IOptionsMonitor<RabbitMqConsumerOptions> consumerOptions, IOptionsMonitor<BoundedContext> boundedContext, IOptionsMonitor<TenantsOptions> tenantsOptionsMonitor, ISubscriberCollection<T> subscriberCollection, IRabbitMqConnectionFactory connectionFactory, BoundedContextRabbitMqNamer bcRabbitMqNamer, ILogger<RabbitMqStartup<T>> logger, QueueBindingArgumentsFactory queueBindingArgumentsFactory)
    {
        this.tenantsOptions = tenantsOptionsMonitor.CurrentValue;
        this.boundedContext = boundedContext.CurrentValue;
        this.subscriberCollection = subscriberCollection;
        this.connectionFactory = connectionFactory;
        this.bcRabbitMqNamer = bcRabbitMqNamer;
        this.logger = logger;
        this.consumerOptions = consumerOptions.CurrentValue;
        this.queueBindingArgumentsFactory = queueBindingArgumentsFactory;

        var type = typeof(T);

        isSystemQueue = typeof(ISystemHandler).IsAssignableFrom(type);
        regularQueueName = bcRabbitMqNamer.Get_QueueName(type, consumerOptions.CurrentValue.FanoutMode);

        tenantsOptionsMonitor.OnChange(async newOptions =>
        {
            if (logger.IsEnabled(LogLevel.Debug))
                this.logger.LogDebug("Tenant options re-loaded with {@options}", newOptions);

            await BootstrapReloadedTenants(tenantsOptions.Tenants, newOptions.Tenants);

            tenantsOptions = newOptions;
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
                await RecoverModelAsync(specialQueueName, channel, [subscriber], tenantsOptions.Tenants).ConfigureAwait(false); // initial start, we expect to start RMQ for all the tenants
            }

            IEnumerable<ISubscriber> theRestOfTheSubscribers = subscriberCollection.Subscribers.Except(subscribersWithDedicatedQueues);
            await RecoverModelAsync(regularQueueName, channel, theRestOfTheSubscribers, tenantsOptions.Tenants).ConfigureAwait(false); // initial start, we expect to start RMQ for all the tenants
        }
    }

    private async Task RecoverModelAsync(string queueName, IChannel channel, IEnumerable<ISubscriber> subscribers, IEnumerable<string> forTenants)
    {
        IEnumerable<string> exchangesToDeclare = queueBindingArgumentsFactory.GetExchangesToDeclare(subscribers, isSystemQueue);

        foreach (string exchange in exchangesToDeclare)
        {
            await channel.ExchangeDeclareAsync(exchange, PipelineType.Headers.ToString(), true).ConfigureAwait(false);
        }

        await channel.QueueDeclareAsync(queueName, true, false, false, null).ConfigureAwait(false);

        List<string> exchangesThatQueueMustBindTo = queueBindingArgumentsFactory.GetExchangesToBindQueueTo(subscribers, isSystemQueue);

        bool thereIsAScheduledQueue = false;
        string scheduledQueue = string.Empty;

        bool isProcessManagerQueue = typeof(T).Name.Equals(typeof(IProcessManager).Name) || typeof(T).Name.Equals(typeof(ISystemProcessManager).Name);
        if (isProcessManagerQueue)
        {
            bool hasOneExchangeGroup = exchangesThatQueueMustBindTo.Count == 1;
            if (hasOneExchangeGroup)
            {
                string targetExchangeAfterTtlExpires = exchangesThatQueueMustBindTo.Single();
                var arguments = new Dictionary<string, object>()
                {
                    { "x-dead-letter-exchange", targetExchangeAfterTtlExpires}
                };

                scheduledQueue = $"{queueName}.Scheduled";
                await channel.QueueDeclareAsync(scheduledQueue, true, false, false, arguments).ConfigureAwait(false);

                thereIsAScheduledQueue = true;
            }
            else if (exchangesThatQueueMustBindTo.Count > 1)
            {
                throw new Exception($"There are more than one exchanges defined for {typeof(T).Name}. RabbitMQ does not allow this functionality and you need to fix one or more of the following subscribers:{Environment.NewLine}{string.Join(Environment.NewLine, subscribers.Select(sub => sub.Id))}");
            }
        }

        bool isTriggerQueue = typeof(T).Name.Equals(typeof(ITrigger).Name);
        if (isTriggerQueue && exchangesThatQueueMustBindTo.Count > 0)
        {
            if (exchangesThatQueueMustBindTo.Count > 1)
            {
                logger.LogWarning("There are more than one exchanges defined for {handlerType}. RabbitMQ does not allow this functionality. We will pick first exchange.", typeof(T).Name);
            }

            string targetExchangeAfterTtlExpires = exchangesThatQueueMustBindTo.First();
            var arguments = new Dictionary<string, object>()
            {
                { "x-dead-letter-exchange", targetExchangeAfterTtlExpires}
            };

            scheduledQueue = $"{queueName}.Scheduled";
            await channel.QueueDeclareAsync(scheduledQueue, true, false, false, arguments).ConfigureAwait(false);

            thereIsAScheduledQueue = true;
        }

        foreach (var exchangeName in exchangesThatQueueMustBindTo)
        {
            // Standard exchange
            await channel.ExchangeDeclareAsync(exchangeName, PipelineType.Headers.ToString(), true, false, null).ConfigureAwait(false);

            if (thereIsAScheduledQueue)
            {
                await channel.ExchangeDeclareAsync($"{exchangeName}.Delayer", ExchangeType.Headers, true, false).ConfigureAwait(false);
            }
        }
        bool isIEventStoreIndex = typeof(T).Name.Equals(typeof(IEventStoreIndex).Name);

        var bindHeadersToExchange = queueBindingArgumentsFactory.GetBindHeadersPerExchangeFor(subscribers, isSystemQueue, isTriggerQueue, isIEventStoreIndex, forTenants, false);

        foreach (var (standardExchangeName, bindHeadersPerExchange) in bindHeadersToExchange)
        {
            foreach (var header in bindHeadersPerExchange)
            {
                await channel.QueueBindAsync(queueName, standardExchangeName, string.Empty, new Dictionary<string, object> { { header.Key, header.Value } }).ConfigureAwait(false);

                if (thereIsAScheduledQueue)
                {
                    await channel.QueueBindAsync(scheduledQueue, $"{standardExchangeName}.Delayer", string.Empty, new Dictionary<string, object> { { header.Key, header.Value } }).ConfigureAwait(false);
                }
            }
        }
    }

    private async Task BootstrapReloadedTenants(IEnumerable<string> previousTenants, IEnumerable<string> newTenantsOptions)
    {
        try
        {
            List<string> removedTenants = previousTenants.Except(newTenantsOptions).ToList();
            List<string> newlyAddedTenants = newTenantsOptions.Except(previousTenants).ToList();

            using (IConnection connection = await connectionFactory.CreateConnectionAsync().ConfigureAwait(false))
            using (var channel = await connection.CreateChannelAsync().ConfigureAwait(false))
            {
                IEnumerable<ISubscriber> subscribersWithDedicatedQueues = subscriberCollection.Subscribers.SubscribersWithDedicatedQueuesOnly();

                foreach (var subscriber in subscribersWithDedicatedQueues)
                {
                    string specialQueueName = bcRabbitMqNamer.Get_QueueName(subscriber.HandlerType, this.consumerOptions.FanoutMode);

                    if (removedTenants.Count > 0)
                    {
                        await RemoveObsoleteBindingsFromRemovedTenants(specialQueueName, channel, [subscriber], removedTenants); // unbind the tenant that was removed from configuration
                    }
                    if (newlyAddedTenants.Count > 0)
                    {
                        await RecoverModelAsync(specialQueueName, channel, [subscriber], newlyAddedTenants).ConfigureAwait(false); // here we start only the tenants with the new state of options after reload
                    }
                }

                IEnumerable<ISubscriber> theRestOfTheSubscribers = subscriberCollection.Subscribers.Except(subscribersWithDedicatedQueues);

                if (removedTenants.Count > 0)
                {
                    await RemoveObsoleteBindingsFromRemovedTenants(regularQueueName, channel, theRestOfTheSubscribers, removedTenants); // unbind the tenant that was removed from configuration
                }
                if (newlyAddedTenants.Count > 0)
                {
                    await RecoverModelAsync(regularQueueName, channel, theRestOfTheSubscribers, newlyAddedTenants).ConfigureAwait(false); // here we start the tenants with the new state of options after reload
                }
            }
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Error occurred while reloading tenant options.");
        }
    }

    private async Task RemoveObsoleteBindingsFromRemovedTenants(string queueName, IChannel channel, IEnumerable<ISubscriber> subscribers, List<string> removedTenants)
    {
        IEnumerable<string> exchangesToDeclare = queueBindingArgumentsFactory.GetExchangesToDeclare(subscribers, isSystemQueue);

        bool isProcessManagerQueue = typeof(T).Name.Equals(typeof(IProcessManager).Name) || typeof(T).Name.Equals(typeof(ISystemProcessManager).Name);
        bool isTriggerQueue = typeof(T).Name.Equals(typeof(ITrigger).Name);
        bool isIEventStoreIndex = typeof(T).Name.Equals(typeof(IEventStoreIndex).Name);

        bool thereIsAScheduledQueue = false;
        var bindHeadersToExchange = queueBindingArgumentsFactory.GetBindHeadersPerExchangeFor(subscribers, isSystemQueue, isTriggerQueue, isIEventStoreIndex, removedTenants, true);

        List<string> exchangesThatQueueMustBindTo = queueBindingArgumentsFactory.GetExchangesToBindQueueTo(subscribers, isSystemQueue);

        if (isProcessManagerQueue)
        {
            if (exchangesThatQueueMustBindTo.Count > 1) // technically there should not be any cases like this one, but we are doing the check just in case...
            {
                throw new Exception($"There are more than one exchanges defined for {typeof(T).Name}. RabbitMQ does not allow this functionality and you need to fix one or more of the following subscribers:{Environment.NewLine}{string.Join(Environment.NewLine, subscribers.Select(sub => sub.Id))}");
            }

            thereIsAScheduledQueue = true;
        }
        if (isTriggerQueue && exchangesThatQueueMustBindTo.Count > 0)
        {
            thereIsAScheduledQueue = true;
        }

        foreach (var (standardExchangeName, bindHeadersPerExchange) in bindHeadersToExchange)
        {
            foreach (var header in bindHeadersPerExchange)
            {
                await channel.QueueUnbindAsync(queueName, standardExchangeName, string.Empty, new Dictionary<string, object> { { header.Key, header.Value } }).ConfigureAwait(false);

                if (thereIsAScheduledQueue)
                {
                    await channel.QueueUnbindAsync($"{queueName}.Scheduled", $"{standardExchangeName}.Delayer", string.Empty, new Dictionary<string, object> { { header.Key, header.Value } }).ConfigureAwait(false);
                }
            }
        }
    }
}

[InceptionStartup(Bootstraps.Configuration)]
public class AppService_Startup : RabbitMqStartup<IApplicationService>
{
    public AppService_Startup(IOptionsMonitor<TenantsOptions> tenantsOptions, IOptionsMonitor<RabbitMqConsumerOptions> consumerOptions, IOptionsMonitor<BoundedContext> boundedContext, ISubscriberCollection<IApplicationService> subscriberCollection, IRabbitMqConnectionFactory connectionFactory, BoundedContextRabbitMqNamer bcRabbitMqNamer, ILogger<AppService_Startup> logger, QueueBindingArgumentsFactory queueBindingArgumentsFactory) : base(consumerOptions, boundedContext, tenantsOptions, subscriberCollection, connectionFactory, bcRabbitMqNamer, logger, queueBindingArgumentsFactory) { }
}

[InceptionStartup(Bootstraps.Configuration)]
public class SystemEventStoreIndex_Startup : RabbitMqStartup<ISystemEventStoreIndex>
{
    public SystemEventStoreIndex_Startup(IOptionsMonitor<TenantsOptions> tenantsOptions, IOptionsMonitor<RabbitMqConsumerOptions> consumerOptions, IOptionsMonitor<BoundedContext> boundedContext, ISubscriberCollection<ISystemEventStoreIndex> subscriberCollection, IRabbitMqConnectionFactory connectionFactory, BoundedContextRabbitMqNamer bcRabbitMqNamer, ILogger<SystemEventStoreIndex_Startup> logger, QueueBindingArgumentsFactory queueBindingArgumentsFactory) : base(consumerOptions, boundedContext, tenantsOptions, subscriberCollection, connectionFactory, bcRabbitMqNamer, logger, queueBindingArgumentsFactory) { }
}

[InceptionStartup(Bootstraps.Configuration)]
public class EventStoreIndex_Startup : RabbitMqStartup<IEventStoreIndex>
{
    public EventStoreIndex_Startup(IOptionsMonitor<TenantsOptions> tenantsOptions, IOptionsMonitor<RabbitMqConsumerOptions> consumerOptions, IOptionsMonitor<BoundedContext> boundedContext, ISubscriberCollection<IEventStoreIndex> subscriberCollection, IRabbitMqConnectionFactory connectionFactory, BoundedContextRabbitMqNamer bcRabbitMqNamer, ILogger<EventStoreIndex_Startup> logger, QueueBindingArgumentsFactory queueBindingArgumentsFactory) : base(consumerOptions, boundedContext, tenantsOptions, subscriberCollection, connectionFactory, bcRabbitMqNamer, logger, queueBindingArgumentsFactory) { }
}

[InceptionStartup(Bootstraps.Configuration)]
public class Projection_Startup : RabbitMqStartup<IProjection>
{
    public Projection_Startup(IOptionsMonitor<TenantsOptions> tenantsOptions, IOptionsMonitor<RabbitMqConsumerOptions> consumerOptions, IOptionsMonitor<BoundedContext> boundedContext, ISubscriberCollection<IProjection> subscriberCollection, IRabbitMqConnectionFactory connectionFactory, BoundedContextRabbitMqNamer bcRabbitMqNamer, ILogger<Projection_Startup> logger, QueueBindingArgumentsFactory queueBindingArgumentsFactory) : base(consumerOptions, boundedContext, tenantsOptions, subscriberCollection, connectionFactory, bcRabbitMqNamer, logger, queueBindingArgumentsFactory) { }
}

[InceptionStartup(Bootstraps.Configuration)]
public class Port_Startup : RabbitMqStartup<IPort>
{
    public Port_Startup(IOptionsMonitor<TenantsOptions> tenantsOptions, IOptionsMonitor<RabbitMqConsumerOptions> consumerOptions, IOptionsMonitor<BoundedContext> boundedContext, ISubscriberCollection<IPort> subscriberCollection, IRabbitMqConnectionFactory connectionFactory, BoundedContextRabbitMqNamer bcRabbitMqNamer, ILogger<Port_Startup> logger, QueueBindingArgumentsFactory queueBindingArgumentsFactory) : base(consumerOptions, boundedContext, tenantsOptions, subscriberCollection, connectionFactory, bcRabbitMqNamer, logger, queueBindingArgumentsFactory) { }
}

[InceptionStartup(Bootstraps.Configuration)]
public class ProcessManager_Startup : RabbitMqStartup<IProcessManager>
{
    public ProcessManager_Startup(IOptionsMonitor<TenantsOptions> tenantsOptions, IOptionsMonitor<RabbitMqConsumerOptions> consumerOptions, IOptionsMonitor<BoundedContext> boundedContext, ISubscriberCollection<IProcessManager> subscriberCollection, IRabbitMqConnectionFactory connectionFactory, BoundedContextRabbitMqNamer bcRabbitMqNamer, ILogger<ProcessManager_Startup> logger, QueueBindingArgumentsFactory queueBindingArgumentsFactory) : base(consumerOptions, boundedContext, tenantsOptions, subscriberCollection, connectionFactory, bcRabbitMqNamer, logger, queueBindingArgumentsFactory) { }
}

[InceptionStartup(Bootstraps.Configuration)]
public class Gateway_Startup : RabbitMqStartup<IGateway>
{
    public Gateway_Startup(IOptionsMonitor<TenantsOptions> tenantsOptions, IOptionsMonitor<RabbitMqConsumerOptions> consumerOptions, IOptionsMonitor<BoundedContext> boundedContext, ISubscriberCollection<IGateway> subscriberCollection, IRabbitMqConnectionFactory connectionFactory, BoundedContextRabbitMqNamer bcRabbitMqNamer, ILogger<Gateway_Startup> logger, QueueBindingArgumentsFactory queueBindingArgumentsFactory) : base(consumerOptions, boundedContext, tenantsOptions, subscriberCollection, connectionFactory, bcRabbitMqNamer, logger, queueBindingArgumentsFactory) { }
}

[InceptionStartup(Bootstraps.Configuration)]
public class TriggerPrivate_Startup : RabbitMqStartup<ITrigger>
{
    public TriggerPrivate_Startup(IOptionsMonitor<TenantsOptions> tenantsOptions, IOptionsMonitor<RabbitMqConsumerOptions> consumerOptions, IOptionsMonitor<BoundedContext> boundedContext, ISubscriberCollection<ITrigger> subscriberCollection, IRabbitMqConnectionFactory connectionFactory, BoundedContextRabbitMqNamer bcRabbitMqNamer, ILogger<TriggerPrivate_Startup> logger, QueueBindingArgumentsFactory queueBindingArgumentsFactory) : base(consumerOptions, boundedContext, tenantsOptions, subscriberCollection, connectionFactory, bcRabbitMqNamer, logger, queueBindingArgumentsFactory) { }
}

[InceptionStartup(Bootstraps.Configuration)]
public class SystemAppService_Startup : RabbitMqStartup<ISystemAppService>
{
    public SystemAppService_Startup(IOptionsMonitor<TenantsOptions> tenantsOptions, IOptionsMonitor<RabbitMqConsumerOptions> consumerOptions, IOptionsMonitor<BoundedContext> boundedContext, ISubscriberCollection<ISystemAppService> subscriberCollection, IRabbitMqConnectionFactory connectionFactory, BoundedContextRabbitMqNamer bcRabbitMqNamer, ILogger<SystemAppService_Startup> logger, QueueBindingArgumentsFactory queueBindingArgumentsFactory) : base(consumerOptions, boundedContext, tenantsOptions, subscriberCollection, connectionFactory, bcRabbitMqNamer, logger, queueBindingArgumentsFactory) { }
}

[InceptionStartup(Bootstraps.Configuration)]
public class SystemProcessManager_Startup : RabbitMqStartup<ISystemProcessManager>
{
    public SystemProcessManager_Startup(IOptionsMonitor<TenantsOptions> tenantsOptions, IOptionsMonitor<RabbitMqConsumerOptions> consumerOptions, IOptionsMonitor<BoundedContext> boundedContext, ISubscriberCollection<ISystemProcessManager> subscriberCollection, IRabbitMqConnectionFactory connectionFactory, BoundedContextRabbitMqNamer bcRabbitMqNamer, ILogger<SystemProcessManager_Startup> logger, QueueBindingArgumentsFactory queueBindingArgumentsFactory) : base(consumerOptions, boundedContext, tenantsOptions, subscriberCollection, connectionFactory, bcRabbitMqNamer, logger, queueBindingArgumentsFactory) { }
}

[InceptionStartup(Bootstraps.Configuration)]
public class SystemPort_Startup : RabbitMqStartup<ISystemPort>
{
    public SystemPort_Startup(IOptionsMonitor<TenantsOptions> tenantsOptions, IOptionsMonitor<RabbitMqConsumerOptions> consumerOptions, IOptionsMonitor<BoundedContext> boundedContext, ISubscriberCollection<ISystemPort> subscriberCollection, IRabbitMqConnectionFactory connectionFactory, BoundedContextRabbitMqNamer bcRabbitMqNamer, ILogger<SystemPort_Startup> logger, QueueBindingArgumentsFactory queueBindingArgumentsFactory) : base(consumerOptions, boundedContext, tenantsOptions, subscriberCollection, connectionFactory, bcRabbitMqNamer, logger, queueBindingArgumentsFactory) { }
}

[InceptionStartup(Bootstraps.Configuration)]
public class SystemTrigger_Startup : RabbitMqStartup<ISystemTrigger>
{
    public SystemTrigger_Startup(IOptionsMonitor<TenantsOptions> tenantsOptions, IOptionsMonitor<RabbitMqConsumerOptions> consumerOptions, IOptionsMonitor<BoundedContext> boundedContext, ISubscriberCollection<ISystemTrigger> subscriberCollection, IRabbitMqConnectionFactory connectionFactory, BoundedContextRabbitMqNamer bcRabbitMqNamer, ILogger<SystemTrigger_Startup> logger, QueueBindingArgumentsFactory queueBindingArgumentsFactory) : base(consumerOptions, boundedContext, tenantsOptions, subscriberCollection, connectionFactory, bcRabbitMqNamer, logger, queueBindingArgumentsFactory) { }
}

[InceptionStartup(Bootstraps.Configuration)]
public class SystemProjection_Startup : RabbitMqStartup<ISystemProjection>
{
    public SystemProjection_Startup(IOptionsMonitor<TenantsOptions> tenantsOptions, IOptionsMonitor<RabbitMqConsumerOptions> consumerOptions, IOptionsMonitor<BoundedContext> boundedContext, ISubscriberCollection<ISystemProjection> subscriberCollection, IRabbitMqConnectionFactory connectionFactory, BoundedContextRabbitMqNamer bcRabbitMqNamer, ILogger<SystemProjection_Startup> logger, QueueBindingArgumentsFactory queueBindingArgumentsFactory) : base(consumerOptions, boundedContext, tenantsOptions, subscriberCollection, connectionFactory, bcRabbitMqNamer, logger, queueBindingArgumentsFactory) { }
}

[InceptionStartup(Bootstraps.Configuration)]
public class MigrationHandler_Startup : RabbitMqStartup<IMigrationHandler>
{
    public MigrationHandler_Startup(IOptionsMonitor<TenantsOptions> tenantsOptions, IOptionsMonitor<RabbitMqConsumerOptions> consumerOptions, IOptionsMonitor<BoundedContext> boundedContext, ISubscriberCollection<IMigrationHandler> subscriberCollection, IRabbitMqConnectionFactory connectionFactory, BoundedContextRabbitMqNamer bcRabbitMqNamer, ILogger<MigrationHandler_Startup> logger, QueueBindingArgumentsFactory queueBindingArgumentsFactory) : base(consumerOptions, boundedContext, tenantsOptions, subscriberCollection, connectionFactory, bcRabbitMqNamer, logger, queueBindingArgumentsFactory) { }
}
