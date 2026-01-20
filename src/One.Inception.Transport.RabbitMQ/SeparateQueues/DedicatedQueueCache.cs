using System;
using System.Collections.Concurrent;
using System.Linq;

namespace One.Inception.Transport.RabbitMQ.SeparateQueues;

public static class DedicatedQueueCache
{
    private static readonly ConcurrentDictionary<Type, bool> typeToSpecialness = new ConcurrentDictionary<Type, bool>();

    public static bool IsDedicatedQueue(this Type messageType)
    {
        bool shouldPersist = false;
        if (typeToSpecialness.TryGetValue(messageType, out shouldPersist) == false)
        {
            shouldPersist = GetAndCacheSpecialnessFromAttribute(messageType);
        }
        return shouldPersist;
    }

    /// <summary>
    /// <see cref="ProjectionEventsPersistenceSetting"/>
    /// </summary>
    /// <param name="messageType"></param>
    /// <returns></returns>
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
