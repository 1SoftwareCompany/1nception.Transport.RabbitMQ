using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;
using System.Threading;
using System.Threading.Tasks;

namespace One.Inception.Transport.RabbitMQ;

public class PublisherChannelResolver : ChannelResolverBase // channels per exchange
{
    private static SemaphoreSlim channelThreadGate = new SemaphoreSlim(1); // Instantiate a Singleton of the Semaphore with a value of 1. This means that only 1 thread can be granted access at a time


    public PublisherChannelResolver(ConnectionResolver connectionResolver) : base(connectionResolver) { }

    public override async Task<IChannel> ResolveAsync(string exchange, IRabbitMqOptions options, string boundedContext)
    {
        string channelKey = $"{boundedContext}_{options.GetType().Name}_{exchange}_{options.Server}".ToLower();
        string connectionKey = $"{options.VHost}_{options.Server}".ToLower();

        IChannel channel = GetExistingChannel(channelKey);

        if (channel is null || channel.IsClosed)
        {
            await channelThreadGate.WaitAsync(10000).ConfigureAwait(false);

            channel = GetExistingChannel(channelKey);

            if (channel?.IsClosed == true)
            {
                channels.Remove(channelKey);
                channel = null;
            }

            if (channel is null)
            {
                IConnection connection = await connectionResolver.ResolveAsync(connectionKey, options).ConfigureAwait(false);
                IChannel scopedChannel = await CreateModelForPublisherAsync(connection).ConfigureAwait(false);
                try
                {
                    if (string.IsNullOrEmpty(exchange) == false)
                    {
                        await scopedChannel.ExchangeDeclarePassiveAsync(exchange).ConfigureAwait(true);
                    }
                }
                catch (OperationInterruptedException)
                {
                    throw;
                    // I have no idea why this code was here.
                    //scopedChannel.Dispose();
                    //scopedChannel = CreateModelForPublisher(connection);
                    //scopedChannel.ExchangeDeclare(exchange, PipelineType.Headers.ToString(), true);
                }

                channels.Add(channelKey, scopedChannel);
            }

            channelThreadGate.Release();
        }

        return GetExistingChannel(channelKey);
    }

    private Task<IChannel> CreateModelForPublisherAsync(IConnection connection)
    {
        var channelOpts = new CreateChannelOptions(publisherConfirmationsEnabled: true, publisherConfirmationTrackingEnabled: true);

        return connection.CreateChannelAsync(channelOpts);
    }
}
