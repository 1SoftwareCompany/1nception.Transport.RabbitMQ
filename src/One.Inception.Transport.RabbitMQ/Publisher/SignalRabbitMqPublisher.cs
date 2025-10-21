using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;

namespace One.Inception.Transport.RabbitMQ.Publisher;

public sealed class SignalRabbitMqPublisher : PublisherBase<ISignal>
{
    private readonly PublisherChannelResolver channelResolver;
    private readonly BoundedContext boundedContext;
    private readonly RabbitMqOptions internalOptions;
    private readonly IRabbitMqNamer rabbitMqNamer;
    private readonly ISerializer serializer;
    private readonly ILogger<SignalRabbitMqPublisher> logger;
    private readonly PublicRabbitMqOptionsCollection options;

    public SignalRabbitMqPublisher(IOptionsMonitor<BoundedContext> boundedContextOptionsMonitor, IOptionsMonitor<RabbitMqOptions> internalOptionsMonitor, IOptionsMonitor<PublicRabbitMqOptionsCollection> optionsMonitor, IRabbitMqNamer rabbitMqNamer, PublisherChannelResolver channelResolver, ISerializer serializer, ILogger<SignalRabbitMqPublisher> logger, IEnumerable<DelegatingPublishHandler> handlers)
        : base(handlers)
    {
        this.boundedContext = boundedContextOptionsMonitor.CurrentValue;
        this.internalOptions = internalOptionsMonitor.CurrentValue;
        this.rabbitMqNamer = rabbitMqNamer;
        options = optionsMonitor.CurrentValue;
        this.channelResolver = channelResolver;
        this.serializer = serializer;
        this.logger = logger;
    }

    protected override async Task<PublishResult> PublishInternalAsync(InceptionMessage message)
    {
        PublishResult publishResult = PublishResult.Initial;

        Type messageType = null;

        try
        {
            string messageBC = message.BoundedContext;
            messageType = message.GetMessageType();
            IEnumerable<string> exchanges = GetExistingExchangesNames(message);
            bool isInternalSignal = boundedContext.Name.Equals(messageBC, StringComparison.OrdinalIgnoreCase); // if the message will be published internally to the same BC

            if (isInternalSignal)
            {
                foreach (var exchange in exchanges)
                {
                    publishResult &= await PublishInternallyAsync(message, messageBC, exchange, internalOptions).ConfigureAwait(false);
                }
            }
            else
            {
                foreach (var exchange in exchanges)
                {
                    publishResult &= await PublishPublicallyAsync(message, messageBC, exchange, options.PublicClustersOptions).ConfigureAwait(false);
                }
            }

            return publishResult;
        }
        catch (Exception ex) when (True(() => logger.LogError(ex, "Unable to publish {messageType}", messageType)))
        {
            return PublishResult.Failed;
        }
    }

    private async Task<PublishResult> PublishInternallyAsync(InceptionMessage message, string boundedContext, string exchange, IRabbitMqOptions internalOptions)
    {
        IChannel exchangeModel = await channelResolver.ResolveAsync(exchange, internalOptions, boundedContext).ConfigureAwait(false);

        BasicProperties props = new BasicProperties();
        props = BuildMessageProperties(props, message);
        props = BuildInternalHeaders(props, message);

        return await PublishUsingChannelAsync(message, exchange, exchangeModel, props).ConfigureAwait(false);
    }

    private async Task<PublishResult> PublishPublicallyAsync(InceptionMessage message, string boundedContext, string exchange, IEnumerable<IRabbitMqOptions> scopedOptions)
    {
        PublishResult publishResult = PublishResult.Initial;

        foreach (var opt in scopedOptions)
        {
            IChannel exchangeModel = await channelResolver.ResolveAsync(exchange, opt, boundedContext).ConfigureAwait(false);

            //IBasicProperties props = exchangeModel.CreateBasicProperties();
            BasicProperties props = new BasicProperties();
            props = BuildMessageProperties(props, message);
            props = BuildPublicHeaders(props, message);

            publishResult &= await PublishUsingChannelAsync(message, exchange, exchangeModel, props).ConfigureAwait(false);
        }

        return publishResult;
    }

    private async Task<PublishResult> PublishUsingChannelAsync(InceptionMessage message, string exchange, IChannel exchangeModel, BasicProperties properties)
    {
        try
        {
            byte[] body = serializer.SerializeToBytes(message);
            await exchangeModel.BasicPublishAsync(exchange, string.Empty, false, properties, body).ConfigureAwait(false);

            logger.LogDebug("Published message to exchange {exchange} with headers {@headers}.", exchange, properties.Headers);

            return new PublishResult(true, true);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Published message to exchange {exchange} has FAILED.", exchange);

            return PublishResult.Failed;
        }
    }

    private BasicProperties BuildMessageProperties(BasicProperties properties, InceptionMessage message)
    {
        string ttl = message.GetTtlMilliseconds();
        if (string.IsNullOrEmpty(ttl) == false)
            properties.Expiration = ttl;
        properties.Persistent = false;
        properties.DeliveryMode = DeliveryModes.Transient;

        return properties;
    }

    private BasicProperties BuildPublicHeaders(BasicProperties properties, InceptionMessage message)
    {
        string contractId = message.GetMessageType().GetContractId();
        string boundedContext = message.BoundedContext;
        string tenant = message.GetTenant();

        properties.Headers = new Dictionary<string, object>();
        properties.Headers.Add($"{contractId}@{tenant}", boundedContext); // tenant specific binding in public signal
        properties.Headers.Add("messageid", message.Id.ToByteArray());

        return properties;
    }

    private BasicProperties BuildInternalHeaders(BasicProperties properties, InceptionMessage message)
    {
        string contractId = message.GetMessageType().GetContractId();
        string boundedContext = message.BoundedContext;

        properties.Headers = new Dictionary<string, object>();
        properties.Headers.Add($"{contractId}", boundedContext); // dont use tenant in internal signal
        properties.Headers.Add("messageid", message.Id.ToByteArray());

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
