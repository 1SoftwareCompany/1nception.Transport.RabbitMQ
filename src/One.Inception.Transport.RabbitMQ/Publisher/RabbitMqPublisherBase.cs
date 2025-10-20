using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;

namespace One.Inception.Transport.RabbitMQ;

public abstract class RabbitMqPublisherBase<TMessage> : Publisher<TMessage> where TMessage : IMessage
{
    private readonly ISerializer serializer;
    private readonly PublisherChannelResolver channelResolver;
    private readonly IRabbitMqNamer rabbitMqNamer;
    private readonly ILogger logger;

    public RabbitMqPublisherBase(ISerializer serializer, PublisherChannelResolver channelResolver, IRabbitMqNamer rabbitMqNamer, IEnumerable<DelegatingPublishHandler> handlers, ILogger logger)
        : base(handlers)
    {
        this.serializer = serializer;
        this.channelResolver = channelResolver;
        this.rabbitMqNamer = rabbitMqNamer;
        this.logger = logger;
    }

    // Old version where we foreach first around exchange names. It is known to be working
    //protected override PublishResult PublishInternal(InceptionMessage message)
    //{
    //    PublishResult publishResult = PublishResult.Initial;

    //    string boundedContext = message.BoundedContext;

    //    IEnumerable<string> exchanges = GetExistingExchangesNames(message);
    //    foreach (string exchange in exchanges)
    //    {
    //        IEnumerable<IRabbitMqOptions> scopedOptions = GetOptionsFor(message);
    //        foreach (IRabbitMqOptions scopedOpt in scopedOptions)
    //        {
    //            publishResult &= Publish(message, boundedContext, exchange, scopedOpt);
    //        }
    //    }

    //    return publishResult;
    //}

    protected override async Task<PublishResult> PublishInternalAsync(InceptionMessage message)
    {
        PublishResult publishResult = PublishResult.Initial;

        string boundedContext = message.BoundedContext;

        IEnumerable<string> exchanges = GetExistingExchangesNames(message);

        IEnumerable<IRabbitMqOptions> scopedOptions = GetOptionsFor(message);

        foreach (IRabbitMqOptions scopedOpt in scopedOptions)
        {
            foreach (string exchange in exchanges)
            {
                PublishResult resultNow = await PublishAsync(message, boundedContext, exchange, scopedOpt).ConfigureAwait(false);
                publishResult &= resultNow;
            }
        }

        return publishResult;
    }

    protected abstract IEnumerable<IRabbitMqOptions> GetOptionsFor(InceptionMessage message);

    private async Task<PublishResult> PublishAsync(InceptionMessage message, string boundedContext, string exchange, IRabbitMqOptions options)
    {
        try
        {
            IChannel exchangeChannel = await channelResolver.ResolveAsync(exchange, options, boundedContext).ConfigureAwait(false);
            BasicProperties props = new BasicProperties();
            props = BuildMessageProperties(props, message);
            props = AttachHeaders(props, message);

            byte[] body = serializer.SerializeToBytes(message);
            await exchangeChannel.BasicPublishAsync(exchange, string.Empty, false, props, body).ConfigureAwait(false);

            if(logger.IsEnabled(LogLevel.Debug))
                logger.LogDebug("Published message to exchange {exchange} with headers {@headers}.", exchange, props.Headers);

            return new PublishResult(true, true);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Published message to exchange {exchange} has FAILED.", exchange);

            return PublishResult.Failed;
        }
    }

    protected virtual BasicProperties BuildMessageProperties(BasicProperties properties, InceptionMessage message)
    {
        properties.Headers = new Dictionary<string, object>();
        properties.Headers.Add("messageid", message.Id.ToByteArray());
        string ttl = message.GetTtlMilliseconds();
        if (string.IsNullOrEmpty(ttl) == false)
            properties.Expiration = ttl;
        properties.Persistent = true;

        return properties;
    }

    protected virtual BasicProperties AttachHeaders(BasicProperties properties, InceptionMessage message)
    {
        string boundedContext = message.BoundedContext;

        properties.Headers.Add(message.GetMessageType().GetContractId(), boundedContext);

        return properties;
    }

    private IEnumerable<string> GetExistingExchangesNames(InceptionMessage message)
    {
        Type messageType = message.GetMessageType();

        IEnumerable<string> exchanges = rabbitMqNamer.Get_PublishTo_ExchangeNames(messageType);

        if (string.IsNullOrEmpty(message.GetTtlMilliseconds()) == false)
            exchanges = exchanges.Select(e => $"{e}.Delayer");

        return exchanges;
    }
}
