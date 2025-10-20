using One.Inception.MessageProcessing;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using System.Threading.Tasks;

namespace One.Inception.Transport.RabbitMQ;

/// <summary>
/// Transient secured consumer with some extra connection management.
/// </summary>
/// <typeparam name="TSubscriber"></typeparam>
public class AsyncConsumer<TSubscriber> : AsyncConsumerBase<TSubscriber>
{
    private readonly string queue;

    public AsyncConsumer(string queue, IChannel channel, ISubscriberCollection<TSubscriber> subscriberCollection, ISerializer serializer, ILogger logger)
        : base(channel, subscriberCollection, serializer, logger)
    {
        this.queue = queue;
    }

    public override async Task StartAsync()
    {
        await channel.BasicQosAsync(0, 1, false).ConfigureAwait(false); // prefetch allow to avoid buffer of messages on the flight
        await channel.BasicConsumeAsync(queue, false, string.Empty, this).ConfigureAwait(false); // we should use autoAck: false to avoid messages loosing

        if (logger.IsEnabled(LogLevel.Debug))
            logger.LogDebug("Consumer for {subscriber} started.", typeof(TSubscriber).Name);
    }
}
