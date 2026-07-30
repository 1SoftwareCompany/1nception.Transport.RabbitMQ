using System;
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
    private readonly TOptions options;

    public RabbitMqConnectionFactory(IOptionsMonitor<TOptions> optionsMonitor, ILogger<RabbitMqConnectionFactory<TOptions>> logger)
    {
        options = optionsMonitor.CurrentValue;
        this.logger = logger;
    }

    public Task<IConnection> CreateConnectionAsync()
    {
        return CreateConnectionWithOptionsAsync(options);
    }

    public async Task<IConnection> CreateConnectionWithOptionsAsync(IRabbitMqOptions options)
    {
        if (logger.IsEnabled(LogLevel.Debug))
            logger.LogDebug("Loaded RabbitMQ options are {@Options}", options);

        bool tailRecursion = false;

        do
        {
            try
            {
                var connectionFactory = new ConnectionFactory();
                connectionFactory.Port = options.Port;
                connectionFactory.UserName = options.Username;
                connectionFactory.Password = options.Password;
                connectionFactory.VirtualHost = options.VHost;
                connectionFactory.AutomaticRecoveryEnabled = true;
                connectionFactory.Ssl.Enabled = options.UseSsl;
                connectionFactory.EndpointResolverFactory = (_) => MultipleEndpointResolver.ComposeEndpointResolver(options);
                connectionFactory.ClientProvidedName = options.ConnectionKey;

                // Always await within a try/catch to handle possible exceptions
                IConnection newConnection = await connectionFactory.CreateConnectionAsync();
                if (logger.IsEnabled(LogLevel.Information))
                    logger.LogInformation("Successfully created RabbitMQ connection using options {@options}", options);

                KillBill.RecoveryInterval = connectionFactory.NetworkRecoveryInterval;
                KillBill.HeartbeatTimeout = connectionFactory.RequestedHeartbeat;


                return newConnection;
            }
            catch (Exception ex)
            {
                if (ex is BrokerUnreachableException)
                    logger.LogWarning("Failed to create RabbitMQ connection using options {@options}. Retrying...", options);
                else
                    logger.LogWarning(ex, "Failed to create RabbitMQ connection using options {@options}. Retrying...", options);

                tailRecursion = true;
            }

            if (tailRecursion)
                await Task.Delay(5000);

        }
        while (tailRecursion == true);

        return default;
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
