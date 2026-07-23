using RabbitMQ.Client;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace One.Inception.Transport.RabbitMQ;

public interface IChannelResolverBase
{
    Task<IChannel> ResolveAsync(string resolveKey, IRabbitMqOptions options, string boundedContext);
}
