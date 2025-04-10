﻿using System.Collections.Generic;
using RabbitMQ.Client;

namespace One.Inception.Transport.RabbitMQ;

public abstract class ChannelResolverBase
{
    protected readonly Dictionary<string, IModel> channels;
    protected readonly ConnectionResolver connectionResolver;
    protected static readonly object @lock = new object();

    public ChannelResolverBase(ConnectionResolver connectionResolver)
    {
        channels = new Dictionary<string, IModel>();
        this.connectionResolver = connectionResolver;
    }

    public virtual IModel Resolve(string resolveKey, IRabbitMqOptions options, string boundedContext)
    {
        resolveKey = resolveKey.ToLower();

        IModel channel = GetExistingChannel(resolveKey);

        if (channel is null || channel.IsClosed)
        {
            lock (@lock) // Maybe we should make this lock per key?!?
            {
                channel = GetExistingChannel(resolveKey);

                if (channel?.IsClosed == true)
                {
                    channels.Remove(resolveKey);
                    channel = null;
                }

                if (channel is null)
                {
                    var connection = connectionResolver.Resolve(boundedContext, options);
                    IModel scopedChannel = connection.CreateModel();
                    scopedChannel.ConfirmSelect();

                    channels.Add(resolveKey, scopedChannel);
                }
            }
        }

        return GetExistingChannel(resolveKey);
    }

    protected IModel GetExistingChannel(string resolveKey)
    {
        channels.TryGetValue(resolveKey, out IModel channel);

        return channel;
    }
}
