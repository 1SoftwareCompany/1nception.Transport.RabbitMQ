﻿using System.Collections.Generic;
using One.Inception.Discoveries;
using One.Inception.Transport.RabbitMQ.Startup;
using Microsoft.Extensions.DependencyInjection;

namespace One.Inception.Transport.RabbitMQ;

public class RabbitMqConsumerDiscovery : DiscoveryBase<IConsumer<IMessageHandler>>
{
    protected override DiscoveryResult<IConsumer<IMessageHandler>> DiscoverFromAssemblies(DiscoveryContext context)
    {
        return new DiscoveryResult<IConsumer<IMessageHandler>>(GetModels(), services => services
                                                                                .AddOptions<RabbitMqOptions, RabbitMqOptionsProvider>()
                                                                                .AddOptions<RabbitMqConsumerOptions, RabbitMqConsumerOptionsProvider>());
    }

    IEnumerable<DiscoveredModel> GetModels()
    {
        yield return new DiscoveredModel(typeof(IRabbitMqConnectionFactory), typeof(RabbitMqConnectionFactory<RabbitMqOptions>), ServiceLifetime.Singleton);

        var consumerModel = new DiscoveredModel(typeof(IConsumer<>), typeof(Consumer<>), ServiceLifetime.Singleton);
        consumerModel.CanOverrideDefaults = true;
        yield return consumerModel;

        yield return new DiscoveredModel(typeof(SchedulePoker<>), typeof(SchedulePoker<>), ServiceLifetime.Singleton);

        yield return new DiscoveredModel(typeof(ConsumerFactory<>), typeof(ConsumerFactory<>), ServiceLifetime.Singleton);

        yield return new DiscoveredModel(typeof(RabbitMqInfrastructure), typeof(RabbitMqInfrastructure), ServiceLifetime.Singleton);

        yield return new DiscoveredModel(typeof(ConsumerPerQueueChannelResolver), typeof(ConsumerPerQueueChannelResolver), ServiceLifetime.Singleton);
    }
}
