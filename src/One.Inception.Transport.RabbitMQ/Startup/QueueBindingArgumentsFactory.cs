using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Extensions.Options;
using One.Inception.MessageProcessing;

namespace One.Inception.Transport.RabbitMQ.Startup;

public class QueueBindingArgumentsFactory
{
    private readonly BoundedContextRabbitMqNamer bcRabbitMqNamer;
    private readonly BoundedContext boundedContext;

    public QueueBindingArgumentsFactory(BoundedContextRabbitMqNamer bcRabbitMqNamer, IOptions<BoundedContext> boundedContext)
    {
        this.bcRabbitMqNamer = bcRabbitMqNamer;
        this.boundedContext = boundedContext.Value;
    }

    internal IEnumerable<string> GetExchangesToDeclare(IEnumerable<ISubscriber> subscribers, bool isSystemQueue)
    {
        var messageTypes = subscribers.SelectMany(x => x.GetInvolvedMessageTypes()).Where(mt => typeof(ISystemMessage).IsAssignableFrom(mt) == isSystemQueue).Distinct().ToList();

        IEnumerable<string> exchangesToDeclare = messageTypes
            .SelectMany(bcRabbitMqNamer.Get_ExchangeNames_To_Declare)
            .Distinct();

        return exchangesToDeclare;
    }

    internal List<string> GetExchangesToBindQueueTo(IEnumerable<ISubscriber> subscribers, bool isSystemQueue)
    {
        var messageTypes = subscribers.SelectMany(x => x.GetInvolvedMessageTypes()).Where(mt => typeof(ISystemMessage).IsAssignableFrom(mt) == isSystemQueue).Distinct().ToList();

        var exchangesToBindTheQueueTo = messageTypes
             .SelectMany(bcRabbitMqNamer.Get_BindTo_ExchangeNames)
             .Distinct()
             .ToList();

        return exchangesToBindTheQueueTo;
    }

    internal Dictionary<string, Dictionary<string, object>> GetBindHeadersPerExchangeFor(IEnumerable<ISubscriber> subscribers, bool isSystemQueue, bool isTriggerQueue, bool isIEventStoreIndex, IEnumerable<string> tenants, bool getOnlyPublicBindHeaders)
    {
        var messageTypes = subscribers.SelectMany(x => x.GetInvolvedMessageTypes()).Where(mt => typeof(ISystemMessage).IsAssignableFrom(mt) == isSystemQueue).Distinct().ToList();

        var bindToExchangeGroups = messageTypes
          .SelectMany(mt => bcRabbitMqNamer.Get_BindTo_ExchangeNames(mt).Select(x => new { Exchange = x, MessageType = mt }))
          .GroupBy(x => x.Exchange)
          .Distinct()
          .ToList();

        Dictionary<string, Dictionary<string, List<string>>> event2Handler = BuildEventToHandler(subscribers, isSystemQueue, boundedContext.Name);

        var headersByExchange = new Dictionary<string, Dictionary<string, object>>();

        foreach (var exchangeGroup in bindToExchangeGroups)
        {
            string standardExchangeName = exchangeGroup.Key;

            var bindHeaders = new Dictionary<string, object>();

            foreach (Type msgType in exchangeGroup.Select(x => x.MessageType))
            {
                string contractId = msgType.GetContractId();
                string bc = msgType.GetBoundedContext(boundedContext.Name);

                bool isPublicEventForIndex = isIEventStoreIndex && typeof(IPublicEvent).IsAssignableFrom(msgType); // public event that needs to be handled in index, so we need public bindings with prefixed tenant
                bool isFromAnotherBoundedContext = bc.Equals(boundedContext.Name, StringComparison.OrdinalIgnoreCase) == false;

                bool needsTenantSpecificHeadersForOutsideBC = isTriggerQueue || isPublicEventForIndex || isFromAnotherBoundedContext;
                bool needsHeadersForCurrentBC = getOnlyPublicBindHeaders == false && (isTriggerQueue || needsTenantSpecificHeadersForOutsideBC == false); //because we can have signals in the same BC and ALSO between diff systems.

                List<string> handlers = event2Handler[standardExchangeName][contractId];

                if (needsHeadersForCurrentBC)
                {
                    BuildHeadersForMessageTypeForCurrentBC(contractId, bc, bindHeaders, handlers);
                }

                if (needsTenantSpecificHeadersForOutsideBC)
                {
                    if (getOnlyPublicBindHeaders)
                    {
                        BuildOnlyTenantHeadersForMessageTypeOutsideCurrentBC(contractId, bc, bindHeaders, handlers, tenants);
                    }
                    else
                    {
                        BuildHeadersForMessageTypeOutsideCurrentBCRegular(contractId, bc, bindHeaders, handlers, tenants);
                    }
                }
            }
            headersByExchange[standardExchangeName] = bindHeaders;
        }

        return headersByExchange;
    }

    private Dictionary<string, Dictionary<string, List<string>>> BuildEventToHandler(IEnumerable<ISubscriber> subscribers, bool isSystemQueue, string boundedContext)
    {
        var event2Handler = new Dictionary<string, Dictionary<string, List<string>>>();

        foreach (ISubscriber subscriber in subscribers)
        {
            foreach (Type msgType in subscriber.GetInvolvedMessageTypes().Where(mt => typeof(ISystemMessage).IsAssignableFrom(mt) == isSystemQueue))
            {
                string bc = msgType.GetBoundedContext(boundedContext);
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

    private void BuildHeadersForMessageTypeOutsideCurrentBCRegular(string messageContractId, string currentBC, Dictionary<string, object> headersRef, List<string> handlers, IEnumerable<string> tenants)
    {
        headersRef.TryAdd(messageContractId, currentBC);

        foreach (string tenant in tenants)
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

    private void BuildOnlyTenantHeadersForMessageTypeOutsideCurrentBC(string messageContractId, string currentBC, Dictionary<string, object> headersRef, List<string> handlers, IEnumerable<string> tenants)
    {
        //headersRef.TryAdd(messageContractId, currentBC); I think that was backwards compatability when we were not sending the tenant for public messages. But in this case I think it is an obsolete binding

        foreach (string tenant in tenants)
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
