using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;

namespace One.Inception.Transport.RabbitMQ;

public class ConnectionResolver : IDisposable
{
    private readonly ConcurrentDictionary<string, IConnection> connectionsPerVHost;

    private readonly ConcurrentDictionary<string, SemaphoreSlim> gatesPerConnectionKeyCreation = new();
    private readonly ConcurrentDictionary<string, SemaphoreSlim> gatesForRecoveryWait = new();

    private readonly IRabbitMqConnectionFactory connectionFactory;
    private readonly ILogger<ConnectionResolver> logger;

    public ConnectionResolver(IRabbitMqConnectionFactory connectionFactory, ILogger<ConnectionResolver> logger, CancellationToken cancellationToken = default)
    {
        connectionsPerVHost = new ConcurrentDictionary<string, IConnection>();
        this.connectionFactory = connectionFactory;
        this.logger = logger;
    }

    public async Task<IConnection> ResolveAsync(string key, IRabbitMqOptions options, CancellationToken cancellationToken = default)
    {
        IConnection connection = GetExistingConnection(key);
        if (connection is not null)
        {
            if (connection.IsOpen)
                return connection;

            SemaphoreSlim recoveryGate = gatesForRecoveryWait.GetOrAdd(key, _ => new SemaphoreSlim(1, 1));
            await recoveryGate.WaitAsync(cancellationToken).ConfigureAwait(false);

            try
            {
                while (connection.IsOpen == false)
                {
                    logger.LogError("Connection to RMQ is down... Automatic attempt to auto recover is in process... Will check again after 5 seconds. Key: {connectionKey}", key);
                    await Task.Delay(5000, cancellationToken).ConfigureAwait(false);

                    if (connection.IsOpen)
                    {
                        logger.LogInformation("Connection to RMQ is open after recovery... Key: {connectionKey}", key);
                        return connection;
                    }
                }
            }
            finally
            {
                recoveryGate.Release();
            }
        }

        SemaphoreSlim lockPerConnection = gatesPerConnectionKeyCreation.GetOrAdd(key, _ => new SemaphoreSlim(1, 1));
        await lockPerConnection.WaitAsync(cancellationToken).ConfigureAwait(false);

        try
        {
            connection = GetExistingConnection(key);
            if (connection is not null && connection.IsOpen)
                return connection;

            connection = await CreateConnectionAsync(key, options).ConfigureAwait(false);
            return connection;
        }
        finally
        {
            lockPerConnection.Release();
        }
    }

    private IConnection GetExistingConnection(string key)
    {
        connectionsPerVHost.TryGetValue(key, out IConnection connection);

        return connection;
    }

    private async Task<IConnection> CreateConnectionAsync(string key, IRabbitMqOptions options)
    {
        IConnection connection = await connectionFactory.CreateConnectionWithOptionsAsync(options).ConfigureAwait(false);
        connectionsPerVHost.TryAdd(key, connection);

        SubscribeToConnectionEvents(key, connection);
        return connection;
    }

    private void SubscribeToConnectionEvents(string key, IConnection connection)
    {
        connection.ConnectionRecoveryErrorAsync += (sender, ea) =>
        {
            logger.LogError(ea.Exception, "RabbitMQ auto-recovery FAILED for connection {ConnectionKey}. The connection may never reopen on its own, but probably will after some time...", key);
            return Task.CompletedTask;
        };

        connection.ConnectionShutdownAsync += (sender, ea) =>
        {
            logger.LogError("RabbitMQ connection {ConnectionKey} shut down. Initiator={Initiator}, Code={ReplyCode}, Text={ReplyText}", key, ea.Initiator, ea.ReplyCode, ea.ReplyText);
            return Task.CompletedTask;
        };

        connection.CallbackExceptionAsync += (sender, ea) =>
        {
            logger.LogError(ea.Exception, "RabbitMQ callback exception on connection {ConnectionKey}.", key);
            return Task.CompletedTask;
        };

        // CRITICAL BLIND SPOT: when blocked, IsOpen stays TRUE but the broker has stopped
        // reading the socket. Publishes stall silently. No IsBlocked property exists to poll.
        connection.ConnectionBlockedAsync += (sender, ea) =>
        {
            logger.LogCritical("RabbitMQ connection {ConnectionKey} was BLOCKED by the broker. Reason={Reason}. Publishes will stall until the resource alarm clears.", key, ea.Reason);
            return Task.CompletedTask;
        };

        connection.ConnectionUnblockedAsync += (sender, ea) =>
        {
            logger.LogWarning("RabbitMQ connection {ConnectionKey} was unblocked by the broker. Publishing can resume.", key);
            return Task.CompletedTask;
        };

    }

    public void Dispose()
    {
        foreach (var connection in connectionsPerVHost)
        {
            connection.Value.CloseAsync(TimeSpan.FromSeconds(5));
        }
    }
}
