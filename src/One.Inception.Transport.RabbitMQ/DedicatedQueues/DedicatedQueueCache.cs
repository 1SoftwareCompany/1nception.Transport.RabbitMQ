using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using One.Inception.MessageProcessing;

namespace One.Inception.Transport.RabbitMQ.DedicatedQueues;

internal static class DedicatedQueueCache
{
    private static readonly ConcurrentDictionary<Type, bool> typeToSpecialness = new ConcurrentDictionary<Type, bool>();

    internal static IEnumerable<ISubscriber> SubscribersWithDedicatedQueuesOnly(this IEnumerable<ISubscriber> subscribers)
    {
        if (subscribers is null)
            yield break;

        foreach (var subscriber in subscribers)
        {
            var attr = subscriber.HandlerType
                .GetCustomAttributes(true).Where(attr => attr is DedicatedQueueAttribute)
                .SingleOrDefault() as DedicatedQueueAttribute;

            if (attr is not null)
            {
                typeToSpecialness.TryAdd(subscriber.HandlerType, true);
                yield return subscriber;
            }
            else
            {
                typeToSpecialness.TryAdd(subscriber.HandlerType, false);
                yield return subscriber;
            }
        }
    }

    private static bool GetAndCacheSpecialnessFromAttribute(Type type)
    {
        var attr = type
            .GetCustomAttributes(true).Where(attr => attr is DedicatedQueueAttribute)
            .SingleOrDefault() as DedicatedQueueAttribute;

        if (attr is not null)
        {
            typeToSpecialness.TryAdd(type, true);
            return true;
        }
        else
        {
            typeToSpecialness.TryAdd(type, false);
            return false;
        }
    }
}
