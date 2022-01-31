﻿using Elders.Cronus.MessageProcessing;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace Elders.Cronus.Transport.RabbitMQ
{
    public class AsyncConsumerFactory<T>
    {
        private readonly ILogger logger = CronusLogger.CreateLogger(typeof(AsyncConsumerFactory<>));
        private readonly ConsumerPerQueueChannelResolver channelResolver;
        private readonly ISerializer serializer;
        private readonly RabbitMqConsumerOptions consumerOptions;
        private readonly BoundedContext boundedContext;
        private readonly ISubscriberCollection<T> subscriberCollection;
        private readonly IRabbitMqConnectionFactory connectionFactory;
        private readonly ConcurrentBag<AsyncConsumer<T>> consumers = new ConcurrentBag<AsyncConsumer<T>>();
        private readonly string queueName;
        RabbitMqOptions options;

        public AsyncConsumerFactory(IOptionsMonitor<RabbitMqOptions> optionsMonitor, ConsumerPerQueueChannelResolver channelResolver, IOptionsMonitor<RabbitMqConsumerOptions> consumerOptions, IOptionsMonitor<BoundedContext> boundedContext, ISerializer serializer, ISubscriberCollection<T> subscriberCollection, IRabbitMqConnectionFactory connectionFactory)
        {
            this.options = optionsMonitor.CurrentValue;
            this.boundedContext = boundedContext.CurrentValue;
            this.channelResolver = channelResolver;
            this.serializer = serializer;
            this.consumerOptions = consumerOptions.CurrentValue;
            this.subscriberCollection = subscriberCollection;
            this.connectionFactory = connectionFactory;

            queueName = GetQueueName(this.boundedContext.Name, this.consumerOptions.FanoutMode);
        }

        //public Task CreateConsumersAsync()
        //{
        //    connection = connectionFactory.CreateConnectionWithChannels(consumerOptions.WorkersCount, out List<IModel> channels);

        //    if (connection.IsOpen)
        //    {
        //        foreach (var channel in channels)
        //        {
        //            AsyncConsumer<T> asyncListener = new AsyncConsumer<T>(queueName, channel, subscriberCollection, serializer, logger);
        //            consumers.Add(asyncListener);
        //        }
        //    }

        //    return Task.CompletedTask;
        //}

        public Task CreateConsumersAsync()
        {
            for (int i = 0; i < consumerOptions.WorkersCount; i++)
            {
                IModel channel = channelResolver.Resolve($"{boundedContext.Name}_{typeof(T).Name}_{i}", options, boundedContext.Name);


                AsyncConsumer<T> asyncListener = new AsyncConsumer<T>(queueName, channel, subscriberCollection, serializer, logger);
                consumers.Add(asyncListener);
            }

            return Task.CompletedTask;
        }

        private string GetQueueName(string boundedContext, bool useFanoutMode = false)
        {
            if (useFanoutMode)
            {
                return $"{boundedContext}.{typeof(T).Name}.{Environment.MachineName}";
            }
            else
            {
                string systemMarker = typeof(ISystemHandler).IsAssignableFrom(typeof(T)) ? "cronus." : string.Empty;
                // This is the default
                return $"{boundedContext}.{systemMarker}{typeof(T).Name}";
            }
        }

    }
}

