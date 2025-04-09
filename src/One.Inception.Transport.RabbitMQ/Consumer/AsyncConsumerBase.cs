using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using One.Inception.MessageProcessing;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace One.Inception.Transport.RabbitMQ;

public abstract class AsyncConsumerBase : AsyncEventingBasicConsumer
{
    protected readonly ILogger logger;
    protected readonly ISerializer serializer;
    protected readonly IModel model;
    private bool isСurrentlyConsuming;

    public AsyncConsumerBase(IModel model, ISerializer serializer, ILogger logger) : base(model)
    {
        this.model = model;
        this.serializer = serializer;
        this.logger = logger;
        isСurrentlyConsuming = false;
        Received += AsyncListener_Received;
    }

    protected abstract Task DeliverMessageToSubscribersAsync(BasicDeliverEventArgs ev, AsyncEventingBasicConsumer consumer);

    public async Task StopAsync()
    {
        // 1. We detach the listener so ther will be no new messages coming from the queue
        Received -= AsyncListener_Received;

        // 2. Wait to handle any messages in progress
        while (isСurrentlyConsuming)
        {
            // We are trying to wait all consumers to finish their current work.
            // Ofcourse the host could be forcibly shut down but we are doing our best.

            await Task.Delay(10).ConfigureAwait(false);
        }

        if (model.IsOpen)
            model.Abort();
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
}

public class AsyncConsumerBase<TSubscriber> : AsyncConsumerBase
{
    private readonly ISubscriberCollection<TSubscriber> subscriberCollection;

    public AsyncConsumerBase(IModel model, ISubscriberCollection<TSubscriber> subscriberCollection, ISerializer serializer, ILogger logger) : base(model, serializer, logger)
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
        InceptionMessage InceptionMessage = null;
        List<Task> deliverTasks = new List<Task>();
        try
        {
            InceptionMessage = serializer.DeserializeFromBytes<InceptionMessage>(ev.Body.ToArray());
            InceptionMessage = ExpandRawPayload(InceptionMessage);
        }
        catch (Exception ex)
        {
            // TODO: send to dead letter exchange/queue
            logger.LogError(ex, "Failed to process message. Failed to deserialize: {}", Convert.ToBase64String(ev.Body.ToArray()));
            Ack(ev, consumer);
            return;
        }

        var subscribers = subscriberCollection.GetInterestedSubscribers(InceptionMessage);

        try
        {
            foreach (var subscriber in subscribers)
            {
                deliverTasks.Add(SafeProcessAsync(subscriber, InceptionMessage));
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
                logger.LogError(ex, "Failed to process {messageType} {@messageData}. Actual errors {errors}", InceptionMessage.GetMessageType(), InceptionMessage, subscriberErrors.ToString());
            }
        ))
        { }
        finally
        {
            Ack(ev, consumer);
        }

        static void Ack(BasicDeliverEventArgs ev, AsyncEventingBasicConsumer consumer)
        {
            if (consumer.Model.IsOpen)
            {
                consumer.Model.BasicAck(ev.DeliveryTag, false);
            }
        }
    }

    protected InceptionMessage ExpandRawPayload(InceptionMessage InceptionMessage)
    {
        if (InceptionMessage.Payload is null && InceptionMessage.PayloadRaw?.Length > 0)
        {
            IMessage payload = serializer.DeserializeFromBytes<IMessage>(InceptionMessage.PayloadRaw);
            return new InceptionMessage(payload, InceptionMessage.Headers);
        }

        return InceptionMessage;
    }
}
