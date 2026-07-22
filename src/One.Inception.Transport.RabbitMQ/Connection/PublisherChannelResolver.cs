using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;

namespace One.Inception.Transport.RabbitMQ;

public class PublisherChannelResolver
{
    private readonly ConcurrentDictionary<string, ConcurrentBag<IChannel>> connectionsWithChannels;
    private readonly ConcurrentDictionary<string, SemaphoreSlim> connectionsWithSlots;
    private readonly ConnectionResolver connectionResolver;

    public PublisherChannelResolver(ConnectionResolver connectionResolver)
    {
        connectionsWithChannels = new ConcurrentDictionary<string, ConcurrentBag<IChannel>>();
        connectionsWithSlots = new ConcurrentDictionary<string, SemaphoreSlim>();
        this.connectionResolver = connectionResolver;
    }

    public async Task<bool> UseChannelAsync(string exchange, IRabbitMqOptions options, string boundedContext, Action<IChannel> publish)
    {
        IChannel channel = default;
        try
        {
            if (connectionsWithSlots.TryGetValue(options.ConnectionKey, out SemaphoreSlim slotsLock) == false)
            {
                var maxAllowedChannelCount = options.MaxChannelsForPublish;
                if (maxAllowedChannelCount < 1)
                    maxAllowedChannelCount = 1;

                slotsLock = new SemaphoreSlim(maxAllowedChannelCount, maxAllowedChannelCount);
                connectionsWithSlots.TryAdd(options.ConnectionKey, slotsLock);
            }

            channel = await RentAsync(options);

            try
            {
                if (string.IsNullOrEmpty(exchange) == false)
                {
                    await channel.ExchangeDeclarePassiveAsync(exchange).ConfigureAwait(true);
                }
            }
            catch (OperationInterruptedException)
            {
                throw;
                // I have no idea why this code was here.
                //scopedChannel.Dispose();
                //scopedChannel = CreateModelForPublisher(connection);
                //scopedChannel.ExchangeDeclare(exchange, PipelineType.Headers.ToString(), true);
            }

            publish(channel);


            return true;
        }
        catch
        {
            return false;
        }
        finally
        {
            Return(options, channel);
        }
    }

    private void Return(IRabbitMqOptions options, IChannel channel)
    {
        try
        {
            if (channel is null || channel.IsClosed)
                return;

            if (connectionsWithChannels.TryGetValue(options.ConnectionKey, out ConcurrentBag<IChannel> idleChannels))
            {
                idleChannels.Add(channel);
            }
            else
            {
                connectionsWithChannels.TryAdd(options.ConnectionKey, new ConcurrentBag<IChannel>());
            }
        }
        finally
        {
            if (connectionsWithSlots.TryGetValue(options.ConnectionKey, out SemaphoreSlim _slotsLock))
            {
                _slotsLock.Release();
            }
        }
    }

    private async Task<IChannel> RentAsync(IRabbitMqOptions options)
    {
        connectionsWithSlots.TryGetValue(options.ConnectionKey, out SemaphoreSlim _slotsLock);

        bool lockIsAcquired = await _slotsLock.WaitAsync(TimeSpan.FromSeconds(options.TimeoutForChannelLease)).ConfigureAwait(false);
        if (lockIsAcquired == false)
        {
            //_logger.LogError("Timed out publishing after {timeout}s waiting. Pool is exhausted. Live channel count: {channelCount}.", _defaultWaitTimeout.TotalSeconds, Volatile.Read(ref channelCreatedCount));
            throw new TimeoutException($"No channel available for publish for {options.TimeoutForChannelLease}s");
        }
        else
        {
            try
            {
                IChannel channel = await TakeHealthyOrCreateAsync(options).ConfigureAwait(false);
                return channel;
            }
            catch
            {
                _slotsLock.Release();
                throw;
            }
        }
    }

    private async Task<IChannel> TakeHealthyOrCreateAsync(IRabbitMqOptions options)
    {
        if (connectionsWithChannels.TryGetValue(options.ConnectionKey, out ConcurrentBag<IChannel> idleChannels))
        {
            while (idleChannels.TryTake(out IChannel candidate))
            {
                if (candidate.IsOpen)
                    return candidate;

                await SafeDisposeAsync(candidate).ConfigureAwait(false);
            }
        }
        else
        {
            connectionsWithChannels.TryAdd(options.ConnectionKey, new ConcurrentBag<IChannel>());
        }

        return await CreateChannelAsync(options).ConfigureAwait(false);
    }

    private async Task SafeDisposeAsync(IChannel channel)
    {
        try
        {
            await channel.CloseAsync().ConfigureAwait(false);
        }
        catch
        {
        }
        finally
        {
            try
            {
                await channel.DisposeAsync().ConfigureAwait(false);

                // int count = Interlocked.Decrement(ref channelCreatedCount);
                //_logger.LogInformation("Disposed RMQ channel. Live channel count: {channelCount}.", count);
            }
            catch
            {
            }
        }
    }

    private async Task<IChannel> CreateChannelAsync(IRabbitMqOptions options)
    {
        var connection = await connectionResolver.ResolveAsync(options).ConfigureAwait(false);
        var channelOpts = new CreateChannelOptions(publisherConfirmationsEnabled: true, publisherConfirmationTrackingEnabled: true);

        return await connection.CreateChannelAsync(channelOpts);
    }
}
