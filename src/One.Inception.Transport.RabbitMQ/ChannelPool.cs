//using System;
//using System.Collections.Concurrent;
//using System.Threading;
//using System.Threading.Tasks;
//using Microsoft.Extensions.Logging;
//using Microsoft.Extensions.Options;
//using RabbitMQ.Client;

//namespace One.Inception.Transport.RabbitMQ;

//public sealed class ChannelPool : IAsyncDisposable
//{
//    private readonly SemaphoreSlim _slotsLock;
//    private readonly ConcurrentBag<IChannel> _idleChannels = new();
//    private readonly ConnectionResolver _connectionResolver;
//    private readonly ILogger<ChannelPool> _logger;

//    private int channelCreatedCount;

//    private readonly int maxAllowedChannelCount;
//    private readonly TimeSpan _defaultWaitTimeout;

//    public ChannelPool(ConnectionResolver connectionResolver, IOptions<RabbitMqOptions> options, ILogger<ChannelPool> logger)
//    {
//        _connectionResolver = connectionResolver;
//        _logger = logger;

//        maxAllowedChannelCount = options.Value.MaxChannelsForPublish;
//        if (maxAllowedChannelCount < 1)
//            throw new ArgumentOutOfRangeException(nameof(maxAllowedChannelCount));

//        _slotsLock = new SemaphoreSlim(maxAllowedChannelCount, maxAllowedChannelCount);

//        _defaultWaitTimeout = TimeSpan.FromSeconds(options.Value.TimeoutForChannelLease);
//        if (_defaultWaitTimeout <= TimeSpan.Zero)
//            throw new ArgumentOutOfRangeException(nameof(_defaultWaitTimeout));
//    }

//    /// <summary>
//    /// rents a channel. Dispose the lease to return it.
//    /// blocks if all channels are in use.
//    /// </summary>
//    public async ValueTask<ChannelLease> RentAsync(IRabbitMqOptions options, string bc, CancellationToken ct = default)
//    {
//        bool lockIsAcquired = await _slotsLock.WaitAsync(_defaultWaitTimeout, ct).ConfigureAwait(false);
//        if (lockIsAcquired == false)
//        {
//            _logger.LogError("Timed out publishing after {timeout}s waiting. Pool is exhausted. Live channel count: {channelCount}.", _defaultWaitTimeout.TotalSeconds, Volatile.Read(ref channelCreatedCount));
//            throw new TimeoutException($"No channel available for publish for {_defaultWaitTimeout.TotalSeconds}s");
//        }
//        else
//        {
//            try
//            {
//                IChannel channel = await TakeHealthyOrCreateAsync(options, bc, ct).ConfigureAwait(false);
//                return new ChannelLease(this, channel);
//            }
//            catch
//            {
//                _slotsLock.Release();
//                throw;
//            }
//        }
//    }

//    private async ValueTask<IChannel> TakeHealthyOrCreateAsync(IRabbitMqOptions options, string bc, CancellationToken ct)
//    {
//        // drain any dead channels sitting in the bag.
//        while (_idleChannels.TryTake(out IChannel candidate))
//        {
//            if (candidate.IsOpen)
//                return candidate;

//            await SafeDisposeAsync(candidate).ConfigureAwait(false);
//        }

//        return await CreateChannelAsync(options, bc).ConfigureAwait(false);
//    }

//    private async ValueTask<IChannel> CreateChannelAsync(IRabbitMqOptions options, string bc)
//    {
//        var channelOpts = new CreateChannelOptions(
//            publisherConfirmationsEnabled: true,
//            publisherConfirmationTrackingEnabled: true
//        );

//        IConnection connection = await _connectionResolver.ResolveAsync(options).ConfigureAwait(false);
//        IChannel scopedChannel = await connection.CreateChannelAsync(channelOpts).ConfigureAwait(false);

//        int count = Interlocked.Increment(ref channelCreatedCount);

//        _logger.LogInformation("Successfully created RMQ channel. Current count: {channelCount}.", count);

//        return scopedChannel;
//    }

//    // called by the lease on dispose.
//    internal void Return(IChannel channel)
//    {
//        try
//        {
//            if (channel.IsClosed)
//            {
//                // fire forget the cleanup
//                _ = SafeDisposeAsync(channel).AsTask();
//            }
//            else
//            {
//                _idleChannels.Add(channel);
//            }
//        }
//        finally
//        {
//            _slotsLock.Release();
//        }
//    }

//    public async ValueTask DisposeAsync()
//    {
//        while (_idleChannels.TryTake(out IChannel channel))
//        {
//            await SafeDisposeAsync(channel).ConfigureAwait(false);
//        }
//    }

//    private async ValueTask SafeDisposeAsync(IChannel channel)
//    {
//        try
//        {
//            await channel.CloseAsync().ConfigureAwait(false);
//        }
//        catch
//        {
//        }
//        finally
//        {
//            try
//            {
//                await channel.DisposeAsync().ConfigureAwait(false);

//                int count = Interlocked.Decrement(ref channelCreatedCount);
//                _logger.LogInformation("Disposed RMQ channel. Live channel count: {channelCount}.", count);
//            }
//            catch
//            {
//            }
//        }
//    }
//}

//public sealed class ChannelLease : IAsyncDisposable
//{
//    private readonly ChannelPool _pool;

//    private bool _returned;

//    public IChannel Channel { get; }

//    internal ChannelLease(ChannelPool pool, IChannel channel)
//    {
//        _pool = pool;
//        Channel = channel;
//    }

//    public ValueTask DisposeAsync()
//    {
//        if (_returned)
//            return ValueTask.CompletedTask;

//        _returned = true;

//        _pool.Return(Channel);
//        return ValueTask.CompletedTask;
//    }
//}
