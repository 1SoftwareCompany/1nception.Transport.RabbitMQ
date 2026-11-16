using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;

namespace One.Inception.Transport.RabbitMQ;

/// <summary>
/// https://www.rabbitmq.com/client-libraries/dotnet-api-guide#connection-recovery
/// </summary>
internal static class KillBill
{
    internal static ushort MaxPublishRetries = 5;
    internal static TimeSpan RecoveryInterval = TimeSpan.FromSeconds(5);
    internal static TimeSpan HeartbeatTimeout = TimeSpan.FromSeconds(60);

    /// <summary>
    /// Calculated recovery timeout based on the heartbeat timeout and recovery interval multiplied by the maximum publish retries.
    /// Keep it that way instead of configuring a value.
    /// </summary>
    internal static TimeSpan TotalRecoveryTimeout = HeartbeatTimeout + RecoveryInterval * MaxPublishRetries;
}

public class RabbitMqConnectionFactory<TOptions> : IRabbitMqConnectionFactory where TOptions : IRabbitMqOptions
{

    private readonly ILogger<RabbitMqConnectionFactory<TOptions>> logger;
    private readonly IOptionsMonitor<TOptions> optionsMonitor;

    public RabbitMqConnectionFactory(IOptionsMonitor<TOptions> optionsMonitor, ILogger<RabbitMqConnectionFactory<TOptions>> logger)
    {
        this.optionsMonitor = optionsMonitor;
        this.logger = logger;
    }

    public Task<IConnection> CreateConnectionAsync(string connectionName = null)
    {
        return CreateConnectionInternalAsync(() => optionsMonitor.CurrentValue, connectionName);
    }

    public Task<IConnection> CreateConnectionWithOptionsAsync(IRabbitMqOptions options, string connectionName = null)
    {
        return CreateConnectionInternalAsync(() => ResolveLatestOptions(options), connectionName);
    }

    private async Task<IConnection> CreateConnectionInternalAsync(Func<IRabbitMqOptions> getOptions, string connectionName = null)
    {
        if (string.IsNullOrEmpty(connectionName))
            connectionName = "temp";

        CancellationTokenSource delayCts = null;
        using IDisposable changeSubscription = optionsMonitor.OnChange(_ => CancelDelay(delayCts));

        while (true)
        {
            IRabbitMqOptions currentOptions = getOptions();
            IConnection connection = await TryConnectAsync(currentOptions, connectionName);
            if (connection is not null)
                return connection;

            delayCts = new CancellationTokenSource();
            await WaitBeforeRetryAsync(delayCts);
            delayCts = null;
        }
    }

    private async Task<IConnection> TryConnectAsync(IRabbitMqOptions currentOptions, string connectionName)
    {
        if (logger.IsEnabled(LogLevel.Debug))
            logger.LogDebug("Loaded RabbitMQ options are {@Options}", currentOptions);

        try
        {
            var connectionFactory = new ConnectionFactory
            {
                Port = currentOptions.Port,
                UserName = currentOptions.Username,
                Password = currentOptions.Password,
                VirtualHost = currentOptions.VHost,
                AutomaticRecoveryEnabled = true,
                EndpointResolverFactory = (_) => MultipleEndpointResolver.ComposeEndpointResolver(currentOptions),
                ClientProvidedName = connectionName
            };
            connectionFactory.Ssl.Enabled = currentOptions.UseSsl;

            // Always await within a try/catch to handle possible exceptions
            IConnection newConnection = await connectionFactory.CreateConnectionAsync();
            if (logger.IsEnabled(LogLevel.Information))
                logger.LogInformation("Successfully created RabbitMQ connection using options {@options}", currentOptions);

            KillBill.RecoveryInterval = connectionFactory.NetworkRecoveryInterval;
            KillBill.HeartbeatTimeout = connectionFactory.RequestedHeartbeat;

            return newConnection;
        }
        catch (Exception ex)
        {
            if (ex is BrokerUnreachableException)
                logger.LogWarning("Failed to create RabbitMQ connection using options {@options}. Retrying...", currentOptions);
            else
                logger.LogWarning(ex, "Failed to create RabbitMQ connection using options {@options}. Retrying...", currentOptions);

            return null;
        }
    }

    private IRabbitMqOptions ResolveLatestOptions(IRabbitMqOptions initialOptions)
    {
        if (initialOptions is not TOptions)
            return initialOptions;

        TOptions current = optionsMonitor.CurrentValue;
        if (current is null)
            return initialOptions;

        if (string.IsNullOrEmpty(initialOptions.BoundedContext) is false)
            return current.GetOptionsFor(initialOptions.BoundedContext) ?? current;

        return current;
    }

    private static async Task WaitBeforeRetryAsync(CancellationTokenSource delayCts)
    {
        try
        {
            await Task.Delay(5000, delayCts.Token);
        }
        catch (OperationCanceledException)
        {
            // Options changed while waiting; retry immediately with updated options.
        }
        finally
        {
            delayCts.Dispose();
        }
    }

    private static void CancelDelay(CancellationTokenSource delayCts)
    {
        try
        {
            delayCts?.Cancel();
        }
        catch (ObjectDisposedException)
        {
            // Delay token was already disposed as the iteration completed.
        }
    }

    private class MultipleEndpointResolver : DefaultEndpointResolver
    {
        MultipleEndpointResolver(AmqpTcpEndpoint[] amqpTcpEndpoints) : base(amqpTcpEndpoints) { }

        public static MultipleEndpointResolver ComposeEndpointResolver(IRabbitMqOptions options)
        {
            AmqpTcpEndpoint[] endpoints = AmqpTcpEndpoint.ParseMultiple(options.Server);

            if (options.UseSsl is false)
                return new MultipleEndpointResolver(endpoints);

            foreach (AmqpTcpEndpoint endp in endpoints)
            {
                endp.Ssl.Enabled = true;
                endp.Ssl.ServerName = options.Server;
            }

            return new MultipleEndpointResolver(endpoints);
        }
    }
}
