using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using One.Inception.MessageProcessing;
using One.Inception.Multitenancy;
using RabbitMQ.Client;

namespace One.Inception.Transport.RabbitMQ.Startup;

[InceptionStartup(Bootstraps.Configuration)]
public class NodeBroadcast_Startup : IInceptionStartup
{
    private readonly BoundedContext boundedContext;
    private readonly ISubscriberCollection<INodeBroadcast> subscriberCollection;
    private readonly IRabbitMqConnectionFactory connectionFactory;
    private readonly BoundedContextRabbitMqNamer bcRabbitMqNamer;
    private readonly ILogger<NodeBroadcast_Startup> logger;
    private readonly RabbitMqConsumerOptions consumerOptions;

    private TenantsOptions tenantsOptions;

    private readonly string regularQueueName;

    public NodeBroadcast_Startup(IOptionsMonitor<RabbitMqConsumerOptions> consumerOptions, IOptionsMonitor<BoundedContext> boundedContext, IOptionsMonitor<TenantsOptions> tenantsOptionsMonitor, ISubscriberCollection<INodeBroadcast> subscriberCollection, IRabbitMqConnectionFactory connectionFactory, BoundedContextRabbitMqNamer bcRabbitMqNamer, ILogger<NodeBroadcast_Startup> logger)
    {
        this.tenantsOptions = tenantsOptionsMonitor.CurrentValue;
        this.boundedContext = boundedContext.CurrentValue;
        this.subscriberCollection = subscriberCollection;
        this.connectionFactory = connectionFactory;
        this.bcRabbitMqNamer = bcRabbitMqNamer;
        this.logger = logger;
        this.consumerOptions = consumerOptions.CurrentValue;

        regularQueueName = bcRabbitMqNamer.Get_NodeBroadcast_QueueName(typeof(INodeBroadcast));

        tenantsOptionsMonitor.OnChange(TenantOptionsChanges);
    }

    public async Task BootstrapAsync()
    {
        await BootstrapInternalAsync(tenantsOptions.Tenants).ConfigureAwait(false);
    }

    public async Task BootstrapAsync(IEnumerable<string> tenants)
    {
        // race condition check
        HashSet<string> allActualTenants = tenantsOptions.Tenants.ToHashSet();
        foreach (string tenant in tenants)
        {
            if (allActualTenants.Contains(tenant) == false) // the OnChange method hasn't fired yet... but in the inception booter it has fired, because we are here... we can fix this by returning the WIP code that needs to be tested, where we can set only the specific public bindings
            {
                allActualTenants.Add(tenant);
            }
        }

        await BootstrapInternalAsync(allActualTenants).ConfigureAwait(false);
    }

    public async Task BootstrapInternalAsync(IEnumerable<string> allTenants)
    {
        using (var connection = await connectionFactory.CreateConnectionAsync().ConfigureAwait(false))
        using (var channel = await connection.CreateChannelAsync().ConfigureAwait(false))
        {
            await RecoverModelAsync(regularQueueName, channel, subscriberCollection.Subscribers, allTenants).ConfigureAwait(false);
        }
    }

    private async Task RecoverModelAsync(string queueName, IChannel channel, IEnumerable<ISubscriber> subscribers, IEnumerable<string> allTenants)
    {
        IEnumerable<Type> messageTypes = subscribers.SelectMany(x => x.GetInvolvedMessageTypes()).Distinct();

        var publishToExchangeGroups = messageTypes
            .SelectMany(mt => bcRabbitMqNamer.Get_ExchangeNames_To_Declare(mt).Select(x => new { Exchange = x, MessageType = mt }))
            .GroupBy(x => x.Exchange)
            .Distinct();

        foreach (var publishExchangeGroup in publishToExchangeGroups)
        {
            await channel.ExchangeDeclareAsync(publishExchangeGroup.Key, PipelineType.Fanout.ToString(), false).ConfigureAwait(false); // The exchange and the queues must not be durable, because they will be per node and the node can be destroyed at any time. The messages will be lost, but this is expected behavior for node broadcast messages.
        }

        await channel.QueueDeclareAsync(queueName, false, false, true, null).ConfigureAwait(false); // The exchange and the queues must not be durable, because they will be per node and the node can be destroyed at any time. The messages will be lost, but this is expected behavior for node broadcast messages.
        foreach (var publishExchangeGroup in publishToExchangeGroups)
        {
            await channel.QueueBindAsync(queueName, publishExchangeGroup.Key, string.Empty).ConfigureAwait(false);
        }
    }

    private void TenantOptionsChanges(TenantsOptions newOptions)
    {
        if (tenantsOptions.Tenants.SequenceEqual(newOptions.Tenants) == false) // Check for difference between tenants and newOptions
        {
            if (logger.IsEnabled(LogLevel.Debug))
                this.logger.LogDebug("Tenant options re-loaded with {@options}", newOptions);

            tenantsOptions = newOptions;
        }
    }
}
