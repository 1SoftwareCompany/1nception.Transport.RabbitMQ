using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using One.Inception.MessageProcessing;

namespace One.Inception.Transport.RabbitMQ;

public class Consumer<T> : IConsumer<T> where T : IMessageHandler
{
    private CancellationTokenSource tokenSource;
    private readonly SemaphoreSlim lifecycleGate = new(1, 1);
    private readonly ILogger logger;
    private readonly ISubscriberCollection<T> subscriberCollection;
    private readonly ConsumerFactory<T> consumerFactory;
    private readonly IHostApplicationLifetime applicationLifetime;
    private readonly IOptionsMonitor<InceptionHostOptions> hostOptions;

    private TimeSpan ShutdownTimeout => hostOptions?.CurrentValue.ShutdownTimeout ?? new InceptionHostOptions().ShutdownTimeout;

    public Consumer(ISubscriberCollection<T> subscriberCollection, ISerializer serializer, ConsumerFactory<T> consumerFactory, ILogger<Consumer<T>> logger, IHostApplicationLifetime applicationLifetime = null, IOptionsMonitor<InceptionHostOptions> hostOptions = null)
    {
        this.subscriberCollection = subscriberCollection ?? throw new ArgumentNullException(nameof(subscriberCollection));
        ArgumentNullException.ThrowIfNull(serializer);
        this.consumerFactory = consumerFactory;
        this.logger = logger;
        this.applicationLifetime = applicationLifetime;
        this.hostOptions = hostOptions;
    }

    public async Task StartAsync()
    {
        await lifecycleGate.WaitAsync().ConfigureAwait(false);
        try
        {
            if (tokenSource is not null)
                return;

            if (!subscriberCollection.Subscribers.Any())
            {
                logger.LogInformation("Consumer {messageHandler} not started because there are no subscribers.", typeof(T).Name);
                return;
            }

            tokenSource = CancellationTokenSource.CreateLinkedTokenSource(
                applicationLifetime?.ApplicationStopping ?? CancellationToken.None);
            try
            {
                tokenSource.Token.ThrowIfCancellationRequested();
                await consumerFactory.CreateAndStartConsumersAsync(tokenSource.Token).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                // Log and continue so one failing consumer does not abort host startup; cleanup allows a fresh StartAsync.
                logger.LogError(ex, "Failed to start rabbitmq consumer {messageHandler}.", typeof(T).Name);
                using var deadline = new CancellationTokenSource(ShutdownTimeout);
                try { await StopConsumersAsync(deadline.Token).ConfigureAwait(false); }
                catch (OperationCanceledException) when (deadline.IsCancellationRequested)
                {
                    logger.LogWarning("RabbitMQ consumer {messageHandler} startup cleanup deadline expired.", typeof(T).Name);
                }
                catch (Exception cleanupEx) { logger.LogError(cleanupEx, "Failed to clean up consumers after startup failed."); }
            }
        }
        finally
        {
            lifecycleGate.Release();
        }
    }

    public async Task StopAsync(CancellationToken cancellationToken = default)
    {
        using var timeout = cancellationToken.CanBeCanceled ? null : new CancellationTokenSource(ShutdownTimeout);
        CancellationToken deadline = timeout?.Token ?? cancellationToken;

        try
        {
            await lifecycleGate.WaitAsync(deadline).ConfigureAwait(false);
            try
            {
                await StopConsumersAsync(deadline).ConfigureAwait(false);
            }
            finally
            {
                lifecycleGate.Release();
            }
        }
        catch (OperationCanceledException) when (deadline.IsCancellationRequested)
        {
            logger.LogWarning("RabbitMQ consumer {messageHandler} shutdown deadline expired. Unfinished handlers may be redelivered.", typeof(T).Name);
        }
    }

    private async Task StopConsumersAsync(CancellationToken deadline)
    {
        CancellationTokenSource source = tokenSource;
        if (source is null)
            return;

        tokenSource = null;
        Task cancellation = CancelAndDisposeAsync(source);
        await consumerFactory.StopAsync(deadline).WaitAsync(deadline).ConfigureAwait(false);
        await cancellation.WaitAsync(deadline).ConfigureAwait(false);
    }

    private async Task CancelAndDisposeAsync(CancellationTokenSource source)
    {
        try { await source.CancelAsync().ConfigureAwait(false); }
        catch (Exception ex) { logger.LogError(ex, "A consumer cancellation callback failed."); }
        finally { source.Dispose(); }
    }
}
