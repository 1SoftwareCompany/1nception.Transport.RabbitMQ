using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;
using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;

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

    private SemaphoreSlim GetSlotLock(IRabbitMqOptions options)
    {
        SemaphoreSlim slotsLock = null;

        if (connectionsWithSlots.TryGetValue(options.ConnectionKey, out slotsLock) == false)
        {
            slotsLock = new SemaphoreSlim(options.MaxChannelsForPublish, options.MaxChannelsForPublish);
            connectionsWithSlots.TryAdd(options.ConnectionKey, slotsLock);
        }

        return slotsLock;
    }

    public async Task<bool> UseChannelAsync(string exchange, IRabbitMqOptions options, string boundedContext, Func<IChannel, Task> publish)
    {
        if (string.IsNullOrEmpty(exchange)) throw new ArgumentException("Exchange name cannot be null or empty.", nameof(exchange));
        if (options is null) throw new ArgumentNullException(nameof(options));
        if (string.IsNullOrEmpty(boundedContext)) throw new ArgumentNullException(nameof(boundedContext));
        if (publish is null) throw new ArgumentNullException(nameof(publish));

        try
        {
            return await UseChannelWithRetriesAsync(DateTimeOffset.UtcNow, 1, exchange, options, boundedContext, publish);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, $"Failed to publish message to exchange '{exchange}' on bounded context '{boundedContext}' using connection '{options.ConnectionKey}'");
            return false;
        }
    }

    private async Task<bool> UseChannelWithRetriesAsync(DateTimeOffset initialTry, ushort currentTry, string exchange, IRabbitMqOptions options, string boundedContext, Func<IChannel, Task> publish)
    {
        bool shouldRetry = false;
        IChannel channel = null;
        try
        {
            if (currentTry > 1)
                await Task.Delay(KillBill.RecoveryInterval).ConfigureAwait(false);

            channel = await RentAsync(options, exchange);
            await publish(channel);

            return true;
        }
        catch (Exception ex) when (ex is BrokerUnreachableException || ex is AlreadyClosedException || ex is OperationInterruptedException)
        {
            shouldRetry = ShouldRetry(initialTry, currentTry);
            if (shouldRetry)
            {
                logger.LogWarning(ex, $"Failed to publish message to exchange '{exchange}' on bounded context '{boundedContext}' using connection '{options.ConnectionKey}'. Attempt {currentTry} of {KillBill.MaxPublishRetries}. Retrying...");
            }
            else
            {
                logger.LogError(ex, $"Failed to publish message to exchange '{exchange}' on bounded context '{boundedContext}' using connection '{options.ConnectionKey}'. Attempt {currentTry} of {KillBill.MaxPublishRetries}. No more retries left.");
            }
        }
        finally
        {
            await ReturnAsync(options, channel).ConfigureAwait(false);
        }

        if (shouldRetry)
        {
            return await UseChannelWithRetriesAsync(initialTry, currentTry++, exchange, options, boundedContext, publish);
        }

        return false;
    }

    private bool ShouldRetry(DateTimeOffset initialTry, ushort currentTry)
    {
        TimeSpan timeSpentTillNow = DateTimeOffset.UtcNow - initialTry;
        if (currentTry > KillBill.MaxPublishRetries || timeSpentTillNow > KillBill.TotalRecoveryTimeout)
            return false;

        return true;
    }

    private async Task EnsureExchangeExists(string exchange, IChannel channel, string exchangeKey)
    {
        if (declaredExchanges.TryGetValue(exchangeKey, out _) == false)
        {
            await channel.ExchangeDeclarePassiveAsync(exchange).ConfigureAwait(true); // It will do nothing if the exchange already exists and result in a channel-level protocol exception (channel closure) if not.
            declaredExchanges.TryAdd(exchangeKey, channel);
        }
    }

    private async Task ReturnAsync(IRabbitMqOptions options, IChannel channel)
    {
        try
        {
            if (channel is null || channel.IsClosed)
            {
                await SafeDisposeAsync(channel).ConfigureAwait(false);
                return;
            }

            if (connectionsWithChannels.TryGetValue(options.ConnectionKey, out ConcurrentBag<IChannel> idleChannels))
            {
                idleChannels.Add(channel);

                if (logger.IsEnabled(LogLevel.Debug))
                    logger.LogDebug($"Return channel => {options.ConnectionKey} {connectionsWithChannels.Count}/{connectionsWithSlots.Count}");
            }
        }
        finally
        {
            if (connectionsWithSlots.TryGetValue(options.ConnectionKey, out SemaphoreSlim slotsLock))
            {
                if (slotsLock is not null)
                    slotsLock.Release();
            }
        }
    }

    private async Task<IChannel> RentAsync(IRabbitMqOptions options, string exchange)
    {
        SemaphoreSlim slotsLock = GetSlotLock(options);

        bool lockIsAcquired = await slotsLock.WaitAsync(TimeSpan.FromSeconds(options.TimeoutForChannelLease)).ConfigureAwait(false);
        if (lockIsAcquired)
        {
            IChannel channel = await TakeHealthyOrCreateAsync(options, exchange).ConfigureAwait(false);

            return channel;
        }
        else
        {
            throw new Exception($"Channel pool is exhausted.");
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

        async Task<IChannel> CreateChannelAsync(IRabbitMqOptions options)
        {
            var connection = await connectionResolver.ResolveAsync(options).ConfigureAwait(false);
            var channelOpts = new CreateChannelOptions(publisherConfirmationsEnabled: true, publisherConfirmationTrackingEnabled: true);
            if (logger.IsEnabled(LogLevel.Debug))
                logger.LogDebug($"Create channel => {options.ConnectionKey} {connectionsWithChannels.Count}/{connectionsWithSlots.Count}");

            return await connection.CreateChannelAsync(channelOpts);
        }
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
            }
            catch { }
        }
    }
}
