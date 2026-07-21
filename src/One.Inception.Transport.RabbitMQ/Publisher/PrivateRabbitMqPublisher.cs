using System.Collections.Generic;
using One.Inception.Multitenancy;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace One.Inception.Transport.RabbitMQ;

public class PrivateRabbitMqPublisher<TMessage> : RabbitMqPublisherBase<TMessage> where TMessage : IMessage
{
    private readonly IOptionsMonitor<RabbitMqOptions> optionsMonitor;

    public PrivateRabbitMqPublisher(ISerializer serializer, PublisherChannelResolver channelResolver, ITenantResolver<IMessage> tenantResolver, IOptionsMonitor<BoundedContext> boundedContext, IOptionsMonitor<RabbitMqOptions> optionsMonitor, BoundedContextRabbitMqNamer rabbitMqNamer, ILogger<PrivateRabbitMqPublisher<TMessage>> logger, IEnumerable<DelegatingPublishHandler> handlers)
        : base(serializer, channelResolver, rabbitMqNamer, handlers, logger)
    {
        this.optionsMonitor = optionsMonitor;
    }

    protected override IEnumerable<IRabbitMqOptions> GetOptionsFor(InceptionMessage message)
    {
        string boundedContext = message.BoundedContext;
        IRabbitMqOptions scopedOptions = optionsMonitor.CurrentValue.GetOptionsFor(boundedContext);

        yield return scopedOptions;
    }
}
