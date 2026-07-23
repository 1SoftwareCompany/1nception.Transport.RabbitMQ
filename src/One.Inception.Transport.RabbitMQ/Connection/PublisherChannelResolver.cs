using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;

namespace One.Inception.Transport.RabbitMQ;

public class PublisherChannelResolver
{
    private readonly ConnectionResolver connectionResolver;
    private readonly ILogger<PublisherChannelResolver> logger;

    private readonly ConcurrentDictionary<string, ConcurrentBag<IChannel>> connectionsWithChannels;
    private readonly ConcurrentDictionary<string, SemaphoreSlim> connectionsWithSlots;
    private readonly ConcurrentDictionary<string, IChannel> declaredExchanges;

    public PublisherChannelResolver(ConnectionResolver connectionResolver, ILogger<PublisherChannelResolver> logger)
    {
        this.connectionResolver = connectionResolver;
        this.logger = logger;

        connectionsWithChannels = new ConcurrentDictionary<string, ConcurrentBag<IChannel>>();
        connectionsWithSlots = new ConcurrentDictionary<string, SemaphoreSlim>();
        declaredExchanges = new ConcurrentDictionary<string, IChannel>();
    }

    public async Task<bool> UseChannelAsync(string exchange, IRabbitMqOptions options, string boundedContext, Func<IChannel, Task> publish)
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

            channel = await RentAsync(options, exchange);

            try
            {
                if (string.IsNullOrEmpty(exchange) == false)
                {
                    string key = $"{boundedContext}_{exchange}_{options.Server}".ToLower();
                    if (declaredExchanges.TryGetValue(key, out _) == false)
                    {
                        await channel.ExchangeDeclarePassiveAsync(exchange).ConfigureAwait(true); // It will do nothing if the exchange already exists and result in a channel-level protocol exception (channel closure) if not.

                        declaredExchanges.TryAdd(key, channel);
                    }
                }

            }
            catch (OperationInterruptedException)
            {
                throw;
                // I have no idea why this code was here. updated: see comment on line 131. if the exchange does not exist, it throws -> and the catch clause dynamically declares the exchange. this ensures that the messages will at least not be gone
                //scopedChannel.Dispose();
                //scopedChannel = CreateModelForPublisher(connection);
                //scopedChannel.ExchangeDeclare(exchange, PipelineType.Headers.ToString(), true);
            }

            await publish(channel);

            return true;
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failure during acquiring a channel for publish... Published message to exchange {exchange} has FAILED.", exchange);

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
        }
        finally
        {
            if (connectionsWithSlots.TryGetValue(options.ConnectionKey, out SemaphoreSlim slotsLock))
            {
                slotsLock.Release();
            }
        }
    }

    private async Task<IChannel> RentAsync(IRabbitMqOptions options, string exchange)
    {
        connectionsWithSlots.TryGetValue(options.ConnectionKey, out SemaphoreSlim slotsLock);

        bool lockIsAcquired = await slotsLock.WaitAsync(TimeSpan.FromSeconds(options.TimeoutForChannelLease)).ConfigureAwait(false);
        if (lockIsAcquired == false)
        {
            //_logger.LogError("Timed out publishing after {timeout}s waiting. Pool is exhausted. Live channel count: {channelCount}.", options.TimeoutForChannelLease, Volatile.Read(ref channelCreatedCount));
            throw new TimeoutException($"No channel available for publish for {options.TimeoutForChannelLease}s");
        }
        else
        {
            try
            {
                IChannel channel = await TakeHealthyOrCreateAsync(options, exchange).ConfigureAwait(false);

                return channel;
            }
            catch
            {
                slotsLock.Release();
                throw;
            }
        }
    }

    private async Task<IChannel> TakeHealthyOrCreateAsync(IRabbitMqOptions options, string exchange)
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
