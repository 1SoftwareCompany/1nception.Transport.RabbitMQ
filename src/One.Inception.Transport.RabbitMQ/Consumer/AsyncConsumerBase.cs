using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using One.Inception.MessageProcessing;
using One.Inception.Transport.RabbitMQ.DedicatedQueues;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace One.Inception.Transport.RabbitMQ;

public abstract class AsyncConsumerBase : AsyncEventingBasicConsumer
{
    protected readonly ILogger logger;
    protected readonly ISerializer serializer;
    protected readonly IChannel channel;
    private readonly CancellationTokenSource handlingSource;
    private readonly object lifecycleGate = new();
    private readonly TaskCompletionSource drained = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private int inFlight;
    private bool stopping;
    private Task stopTask;

    protected CancellationToken HandlingToken { get; }

    public AsyncConsumerBase(IChannel channel, ISerializer serializer, ILogger logger, CancellationToken cancellationToken = default) : base(channel)
    {
        this.channel = channel;
        this.serializer = serializer;
        this.logger = logger;
        handlingSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        HandlingToken = handlingSource.Token;
        ReceivedAsync += AsyncListener_Received;
    }

    protected abstract Task DeliverMessageToSubscribersAsync(BasicDeliverEventArgs ev, AsyncEventingBasicConsumer consumer);

    public abstract Task StartAsync();

    public Task StopAsync(CancellationToken cancellationToken = default)
    {
        lock (lifecycleGate)
        {
            if (stopTask is not null)
                return stopTask;

            stopping = true;
            if (inFlight == 0)
                drained.TrySetResult();

            return stopTask = StopCoreAsync(cancellationToken);
        }
    }

    private async Task StopCoreAsync(CancellationToken cancellationToken)
    {
        Task cancellation = handlingSource.CancelAsync();
        Task completion = DisposeSourceAfterDrainAsync(cancellation);
        try
        {
            if (channel.IsOpen)
            {
                foreach (string tag in ConsumerTags)
                {
                    // Waiting for cancel-ok can deadlock behind the active delivery callback.
                    await channel.BasicCancelAsync(tag, noWait: true, cancellationToken).ConfigureAwait(false);
                }
            }

            await completion.WaitAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            logger.LogWarning("Consumer shutdown deadline expired. Unfinished deliveries will not be acknowledged.");
        }
        finally
        {
            if (channel.IsOpen)
            {
                try
                {
                    // Closing the channel returns unacknowledged deliveries to RabbitMQ.
                    await channel.AbortAsync().WaitAsync(TimeSpan.FromSeconds(1)).ConfigureAwait(false);
                }
                catch (TimeoutException)
                {
                    logger.LogWarning("Channel shutdown is still waiting for an unfinished delivery callback.");
                }
            }
        }
    }

    private async Task DisposeSourceAfterDrainAsync(Task cancellation)
    {
        try
        {
            try { await cancellation.ConfigureAwait(false); }
            catch (Exception ex) { logger.LogError(ex, "A handler cancellation callback failed."); }

            await drained.Task.ConfigureAwait(false);
        }
        finally
        {
            handlingSource.Dispose();
        }
    }

    private async Task AsyncListener_Received(object sender, BasicDeliverEventArgs @event)
    {
        lock (lifecycleGate)
        {
            if (stopping || HandlingToken.IsCancellationRequested)
                return; // Leave buffered deliveries unacknowledged for channel closure.
            inFlight++;
        }

        try
        {
            using (logger.BeginScope(s =>
            {
                if (@event.BasicProperties.IsHeadersPresent() && @event.BasicProperties.Headers.TryGetValue("messageid", out object messageIdBytes))
                {
                    var messageId = new Guid((byte[])messageIdBytes);
                    s.AddScope("messageid", messageId.ToString());
                }
            }))
            {
                if (sender is AsyncEventingBasicConsumer consumer)
                    await DeliverMessageToSubscribersAsync(@event, consumer).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException) when (HandlingToken.IsCancellationRequested)
        {
            logger.LogInformation("Message handling cancelled during consumer shutdown. Delivery left unacknowledged.");
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to deliver message. MessageData: {data}", Convert.ToBase64String(@event.Body.ToArray()));
            throw;
        }
        finally
        {
            lock (lifecycleGate)
            {
                if (--inFlight == 0 && stopping)
                    drained.TrySetResult();
            }
        }
    }

    protected async Task ProcessMessageAsync(BasicDeliverEventArgs ev, Func<InceptionMessage, IEnumerable<ISubscriber>> resolveSubscribers)
    {
        InceptionMessage message;
        try
        {
            message = ExpandRawPayload(serializer.DeserializeFromBytes<InceptionMessage>(ev.Body.ToArray()));
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to process message. Failed to deserialize: {data}", ev.Body.ToArray());
            await AckAsync(ev).ConfigureAwait(false);
            return;
        }

        List<Task> tasks = new();
        try
        {
            foreach (ISubscriber subscriber in resolveSubscribers(message))
            {
                tasks.Add(SafeProcessAsync(subscriber, message));
            }
            await Task.WhenAll(tasks).ConfigureAwait(false);
        }
        catch (Exception) when (HandlingToken.IsCancellationRequested && tasks.Any(t => t.IsCanceled))
        {
            // WhenAll can throw a fault even when another subscriber was cancelled.
            throw new OperationCanceledException(HandlingToken);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to process {messageType} {@messageData}. Actual errors {errors}",
                message.GetMessageType(), message,
                string.Join(Environment.NewLine, tasks.Where(t => t.IsFaulted).Select(t => t.Exception)));
        }

        // Preserve the existing ordinary-error policy, but never ACK shutdown cancellation.
        await AckAsync(ev).ConfigureAwait(false);
    }

    private Task SafeProcessAsync(ISubscriber subscriber, InceptionMessage message)
    {
        try { return subscriber.ProcessAsync(message, HandlingToken); }
        catch (OperationCanceledException) when (HandlingToken.IsCancellationRequested) { return Task.FromCanceled(HandlingToken); }
        catch (Exception ex) { return Task.FromException(ex); }
    }

    protected async Task AckAsync(BasicDeliverEventArgs ev)
    {
        if (channel.IsOpen)
            await channel.BasicAckAsync(ev.DeliveryTag, false).ConfigureAwait(false);
    }

    protected InceptionMessage ExpandRawPayload(InceptionMessage inceptionMessage)
    {
        if (inceptionMessage.Payload is null && inceptionMessage.PayloadRaw?.Length > 0)
        {
            IMessage payload = serializer.DeserializeFromBytes<IMessage>(inceptionMessage.PayloadRaw);
            return new InceptionMessage(payload, inceptionMessage.Headers);
        }

        return inceptionMessage;
    }
}

public abstract class AsyncConsumerBase<TSubscriber> : AsyncConsumerBase
{
    private readonly ISubscriberCollection<TSubscriber> subscriberCollection;

    public AsyncConsumerBase(IChannel channel, ISubscriberCollection<TSubscriber> subscriberCollection, ISerializer serializer, ILogger logger, CancellationToken cancellationToken = default)
        : base(channel, serializer, logger, cancellationToken)
    {
        this.subscriberCollection = subscriberCollection;
    }

    protected override Task DeliverMessageToSubscribersAsync(BasicDeliverEventArgs ev, AsyncEventingBasicConsumer consumer)
    {
        return ProcessMessageAsync(ev, message => subscriberCollection.GetInterestedSubscribers(message)
            .Except(subscriberCollection.Subscribers.SubscribersWithDedicatedQueuesOnly()));
    }
}

public abstract class AsyncConsumerCustomForSingleSubscriberBase : AsyncConsumerBase
{
    private readonly ISubscriber subscriber;

    public AsyncConsumerCustomForSingleSubscriberBase(IChannel channel, ISubscriber subscriber, ISerializer serializer, ILogger logger, CancellationToken cancellationToken = default)
        : base(channel, serializer, logger, cancellationToken)
    {
        this.subscriber = subscriber;
    }

    protected override Task DeliverMessageToSubscribersAsync(BasicDeliverEventArgs ev, AsyncEventingBasicConsumer consumer)
    {
        return ProcessMessageAsync(ev, _ => new[] { subscriber });
    }
}
