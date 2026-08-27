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

        foreach (ISubscriber subscriber in subscribers)
        {
            bool isDedicated = false;

            if (typeToSpecialness.TryGetValue(subscriber.HandlerType, out isDedicated) == false)
            {
                var attr = subscriber.HandlerType
                    .GetCustomAttributes(true).Where(attr => attr is DedicatedQueueAttribute)
                    .SingleOrDefault() as DedicatedQueueAttribute;

                if (attr is not null)
                {
                    typeToSpecialness.TryAdd(subscriber.HandlerType, true);
                    isDedicated = true;
                }
                else
                {
                    typeToSpecialness.TryAdd(subscriber.HandlerType, false);
                    isDedicated = false;
                }
            }

            if (isDedicated)
            {
                yield return subscriber;
            }
        }
    }
}
