using System.Collections.Generic;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace One.Inception.Transport.RabbitMQ;

public class NodeBroadcastRabbitMqPublisher : RabbitMqPublisherBase<IBroadcast>
{
    private readonly IOptionsMonitor<RabbitMqOptions> optionsMonitor;

    public NodeBroadcastRabbitMqPublisher(IOptionsMonitor<RabbitMqOptions> optionsMonitor, ISerializer serializer, PublisherChannelResolver channelResolver, IRabbitMqNamer rabbitMqNamer, IEnumerable<DelegatingPublishHandler> handlers, ILogger<NodeBroadcastRabbitMqPublisher> logger) : base(serializer, channelResolver, rabbitMqNamer, handlers, logger)
    {
        this.optionsMonitor = optionsMonitor;
    }

    protected override IEnumerable<IRabbitMqOptions> GetOptionsFor(InceptionMessage message)
    {
        string boundedContext = message.BoundedContext;
        IRabbitMqOptions scopedOptions = optionsMonitor.CurrentValue.GetOptionsFor(boundedContext);

        yield return scopedOptions;
    }

    protected override string BuildPublisherConnectionKey(IRabbitMqOptions options) => options.GetConnectionKey(ConnectionResolver.Broadcast);
}
