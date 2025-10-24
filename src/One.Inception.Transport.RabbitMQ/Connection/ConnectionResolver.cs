using RabbitMQ.Client;
using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;

namespace One.Inception.Transport.RabbitMQ;

public class ConnectionResolver : IDisposable
{
    private static SemaphoreSlim connectionThreadGate = new SemaphoreSlim(1); // Instantiate a Singleton of the Semaphore with a value of 1. This means that only 1 thread can be granted access at a time

    private readonly ConcurrentDictionary<string, IConnection> connectionsPerVHost;
    private readonly IRabbitMqConnectionFactory connectionFactory;
    protected static readonly System.Threading.Lock connectionLock = new();

    public ConnectionResolver(IRabbitMqConnectionFactory connectionFactory)
    {
        connectionsPerVHost = new ConcurrentDictionary<string, IConnection>();
        this.connectionFactory = connectionFactory;
    }

    public async Task<IConnection> ResolveAsync(string key, IRabbitMqOptions options)
    {
        IConnection connection = GetExistingConnection(key);

        if (connection is null || connection.IsOpen == false)
        {
            await connectionThreadGate.WaitAsync(5000);

            connection = GetExistingConnection(key);
            if (connection is null || connection.IsOpen == false)
            {
                connection = await CreateConnectionAsync(key, options).ConfigureAwait(true);
            }

            connectionThreadGate.Release();
        }

        return connection;
    }

    private IConnection GetExistingConnection(string key)
    {
        connectionsPerVHost.TryGetValue(key, out IConnection connection);

        return connection;
    }

    private async Task<IConnection> CreateConnectionAsync(string key, IRabbitMqOptions options)
    {
        IConnection connection = await connectionFactory.CreateConnectionWithOptionsAsync(options).ConfigureAwait(false);

        if (connectionsPerVHost.TryGetValue(key, out _))
        {
            if (connectionsPerVHost.TryRemove(key, out _))
                connectionsPerVHost.TryAdd(key, connection);
        }
        else
        {
            connectionsPerVHost.TryAdd(key, connection);
        }

        return connection;
    }

    public void Dispose()
    {
        foreach (var connection in connectionsPerVHost)
        {
            connection.Value.CloseAsync(TimeSpan.FromSeconds(5));
        }
    }
}
