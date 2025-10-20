using RabbitMQ.Client;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace One.Inception.Transport.RabbitMQ;

public abstract class ChannelResolverBase
{
    private static SemaphoreSlim channelThreadGate = new SemaphoreSlim(1); // Instantiate a Singleton of the Semaphore with a value of 1. This means that only 1 thread can be granted access at a time

    protected readonly Dictionary<string, IChannel> channels;
    protected readonly ConnectionResolver connectionResolver;
    protected static readonly System.Threading.Lock _lock = new();

    public ChannelResolverBase(ConnectionResolver connectionResolver)
    {
        channels = new Dictionary<string, IChannel>();
        this.connectionResolver = connectionResolver;
    }

    public virtual async Task<IChannel> ResolveAsync(string resolveKey, IRabbitMqOptions options, string boundedContext)
    {
        resolveKey = resolveKey.ToLower();

        IChannel channel = GetExistingChannel(resolveKey);

        if (channel is null || channel.IsClosed)
        {
            await channelThreadGate.WaitAsync(1000).ConfigureAwait(false);

            channel = GetExistingChannel(resolveKey);

            if (channel?.IsClosed == true)
            {
                channels.Remove(resolveKey);
                channel = null;
            }

            if (channel is null)
            {
                var channelOpts = new CreateChannelOptions(
                    publisherConfirmationsEnabled: true,
                    publisherConfirmationTrackingEnabled: true
                );

                IConnection connection = await connectionResolver.ResolveAsync(boundedContext, options).ConfigureAwait(true);
                IChannel scopedChannel = await connection.CreateChannelAsync(channelOpts).ConfigureAwait(true);

                channels.Add(resolveKey, scopedChannel);
            }

            channelThreadGate.Release();
        }

        return GetExistingChannel(resolveKey);
    }

    protected IChannel GetExistingChannel(string resolveKey)
    {
        channels.TryGetValue(resolveKey, out IChannel channel);

        return channel;
    }
}
