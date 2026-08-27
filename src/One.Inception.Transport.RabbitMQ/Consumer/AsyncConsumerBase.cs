using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
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
    private bool isСurrentlyConsuming;

    public AsyncConsumerBase(IChannel channel, ISerializer serializer, ILogger logger) : base(channel)
    {
        this.channel = channel;
        this.serializer = serializer;
        this.logger = logger;
        isСurrentlyConsuming = false;
        ReceivedAsync += AsyncListener_Received;
    }

    protected abstract Task DeliverMessageToSubscribersAsync(BasicDeliverEventArgs ev, AsyncEventingBasicConsumer consumer);

    public abstract Task StartAsync();

    public async Task StopAsync()
    {
        // 1. We detach the listener so ther will be no new messages coming from the queue
        ReceivedAsync -= AsyncListener_Received;

        // 2. Wait to handle any messages in progress
        while (isСurrentlyConsuming)
        {
            // We are trying to wait all consumers to finish their current work.
            // Ofcourse the host could be forcibly shut down but we are doing our best.
            await Task.Delay(10).ConfigureAwait(false);
        }

        if (channel.IsOpen)
            await channel.AbortAsync();
    }

    private async Task AsyncListener_Received(object sender, BasicDeliverEventArgs @event)
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
            try
            {
                if (logger.IsEnabled(LogLevel.Debug))
                    logger.LogDebug("Message received. Sender {sender}.", sender.GetType().Name);
                isСurrentlyConsuming = true;

                if (sender is AsyncEventingBasicConsumer consumer)
                    await DeliverMessageToSubscribersAsync(@event, consumer).ConfigureAwait(false);
            }
            catch (Exception ex) when (True(() => logger.LogError(ex, $"Failed to deliver message. MesasgeData: {Convert.ToBase64String(@event.Body.ToArray())}")))
            {
                throw;
            }
            finally
            {
                isСurrentlyConsuming = false;
            }
        }
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

    public AsyncConsumerBase(IChannel channel, ISubscriberCollection<TSubscriber> subscriberCollection, ISerializer serializer, ILogger logger) : base(channel, serializer, logger)
    {
        this.subscriberCollection = subscriberCollection;
    }

    private Task SafeProcessAsync(ISubscriber subscriber, InceptionMessage InceptionMessage)
    {
        try
        {
            return subscriber.ProcessAsync(InceptionMessage);
        }
        catch (Exception ex)
        {
            return Task.FromException(ex);
        }
    }

    protected override async Task DeliverMessageToSubscribersAsync(BasicDeliverEventArgs ev, AsyncEventingBasicConsumer consumer)
    {
        InceptionMessage inceptionMessage = null;
        List<Task> deliverTasks = new List<Task>();
        try
        {
            inceptionMessage = serializer.DeserializeFromBytes<InceptionMessage>(ev.Body.ToArray());
            inceptionMessage = ExpandRawPayload(inceptionMessage);
        }
        catch (Exception ex)
        {
            // TODO: send to dead letter exchange/queue
            // TODO: log meta data which is stored in ev.Properties so we know what is the source of the message
            logger.LogError(ex, "Failed to process message. Failed to deserialize: {data}", ev.Body.ToArray());
            await AckAsync(ev, consumer).ConfigureAwait(false);

            return;
        }

        var subscribers = subscriberCollection.GetInterestedSubscribers(inceptionMessage);
        IEnumerable<ISubscriber> subscribersWithDedicatedQueues = subscriberCollection.Subscribers.SubscribersWithDedicatedQueuesOnly();
        var onlyTheTrueSubscribers = subscribers.Except(subscribersWithDedicatedQueues);

        try
        {
            foreach (var subscriber in onlyTheTrueSubscribers)
            {
                deliverTasks.Add(SafeProcessAsync(subscriber, inceptionMessage));
            }

            await Task.WhenAll(deliverTasks).ConfigureAwait(false);
        }
        catch (Exception ex) when (True(() =>
            {
                // Try find some errors
                StringBuilder subscriberErrors = new StringBuilder();
                foreach (Task subscriberCompletedTasks in deliverTasks)
                {
                    if (subscriberCompletedTasks.IsFaulted)
                    {
                        subscriberErrors.AppendLine(subscriberCompletedTasks.Exception.ToString());
                    }
                }
                logger.LogError(ex, "Failed to process {messageType} {@messageData}. Actual errors {errors}", inceptionMessage.GetMessageType(), inceptionMessage, subscriberErrors.ToString());
            }
        ))
        { }
        finally
        {
            await AckAsync(ev, consumer).ConfigureAwait(false);
        }

        async Task AckAsync(BasicDeliverEventArgs ev, AsyncEventingBasicConsumer consumer)
        {
            if (consumer.Channel.IsOpen)
            {
                await consumer.Channel.BasicAckAsync(ev.DeliveryTag, false);
            }
        }
    }
}

public abstract class AsyncConsumerCustomForSingleSubscriberBase : AsyncConsumerBase
{
    private readonly ISubscriber subscriber;

    public AsyncConsumerCustomForSingleSubscriberBase(IChannel channel, ISubscriber subscriber, ISerializer serializer, ILogger logger) : base(channel, serializer, logger)
    {
        this.subscriber = subscriber;
    }

    private Task SafeProcessAsync(ISubscriber subscriber, InceptionMessage InceptionMessage)
    {
        try
        {
            return subscriber.ProcessAsync(InceptionMessage);
        }
        catch (Exception ex)
        {
            return Task.FromException(ex);
        }
    }

    protected override async Task DeliverMessageToSubscribersAsync(BasicDeliverEventArgs ev, AsyncEventingBasicConsumer consumer)
    {
        InceptionMessage inceptionMessage = null;
        try
        {
            inceptionMessage = serializer.DeserializeFromBytes<InceptionMessage>(ev.Body.ToArray());
            inceptionMessage = ExpandRawPayload(inceptionMessage);
        }
        catch (Exception ex)
        {
            // TODO: send to dead letter exchange/queue
            // TODO: log meta data which is stored in ev.Properties so we know what is the source of the message
            logger.LogError(ex, "Failed to process message. Failed to deserialize: {data}", ev.Body.ToArray());
            await AckAsync(ev, consumer).ConfigureAwait(false);

            return;
        }

        Task task = null;
        try
        {
            task = SafeProcessAsync(subscriber, inceptionMessage);

            await task.ConfigureAwait(false);
        }
        catch (Exception ex) when (True(() =>
        {
            // Try find some errors
            StringBuilder subscriberError = new StringBuilder();

            if (task.IsFaulted)
            {
                subscriberError.AppendLine(task.Exception.ToString());
            }
            logger.LogError(ex, "Failed to process {messageType} {@messageData}. Actual errors {errors}", inceptionMessage.GetMessageType(), inceptionMessage, subscriberError.ToString());
        }
        ))
        { }
        finally
        {
            await AckAsync(ev, consumer).ConfigureAwait(false);
        }

        async Task AckAsync(BasicDeliverEventArgs ev, AsyncEventingBasicConsumer consumer)
        {
            if (consumer.Channel.IsOpen)
            {
                await consumer.Channel.BasicAckAsync(ev.DeliveryTag, false);
            }
        }
    }
}
