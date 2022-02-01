﻿using System;
using System.Collections.Concurrent;
using RabbitMQ.Client;

namespace Elders.Cronus.Transport.RabbitMQ
{
    public class PublisherChannelResolver
    {
        private readonly ConcurrentDictionary<string, IModel> channelPerBoundedContext;
        private readonly ConnectionResolver connectionResolver;
        private static readonly object @lock = new object();

        public PublisherChannelResolver(ConnectionResolver connectionResolver)
        {
            channelPerBoundedContext = new ConcurrentDictionary<string, IModel>();
            this.connectionResolver = connectionResolver;
        }

        public IModel Resolve(string boundedContext, IRabbitMqOptions options)
        {
            IModel channel = GetExistingChannel(boundedContext);

            if (channel is null || channel.IsClosed)
            {
                lock (@lock)
                {
                    channel = GetExistingChannel(boundedContext);

                    if (channel is null || channel.IsClosed)
                    {
                        var connection = connectionResolver.Resolve(boundedContext, options);
                        channel = connection.CreateModel();
                        channel.ConfirmSelect();

                        if (channelPerBoundedContext.TryAdd(boundedContext, channel) == false)
                        {
                            throw new Exception("Kak go napravi tova?");
                        }
                    }
                }
            }

            return channel;
        }

        private IModel GetExistingChannel(string boundedContext)
        {
            channelPerBoundedContext.TryGetValue(boundedContext, out IModel channel);

            return channel;
        }
    }
}
