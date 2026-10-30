using System;
using System.Linq;
using System.Collections.Generic;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using System.Runtime.Serialization;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;

namespace One.Inception.Transport.RabbitMQ.RpcAPI;

public interface IRpc
{
    internal Task StartServerAsync();

    internal Task StopConsumersAsync(CancellationToken cancellationToken = default);
}

public interface IRpc<TRequest, TResponse> : IRpc
    where TRequest : IRpcRequest<TResponse>
    where TResponse : IRpcResponse, new()
{
    public Task<TResponse> SendAsync(TRequest request);
}

public class RpcEndpoint<TRequest, TResponse> : IRpc<TRequest, TResponse>
    where TRequest : IRpcRequest<TResponse>
    where TResponse : IRpcResponse, new()
{
    private ResponseConsumer<TRequest, TResponse> client;
    private string clientStartError;
    private readonly List<RequestConsumer<TRequest, TResponse>> servers = new();
    private string route;
    private readonly ConsumerPerQueueChannelResolver channelResolver;
    private readonly BoundedContext boundedContext;
    private readonly RabbitMqConsumerOptions consumerOptions;
    private readonly RabbitMqOptions options;
    private readonly IRequestResponseFactory factory;
    private readonly ISerializer serializer;
    private readonly IServiceProvider serviceProvider;
    private readonly ILogger<RpcEndpoint<TRequest, TResponse>> logger;

    public RpcEndpoint(IOptionsMonitor<RabbitMqOptions> options, IOptionsMonitor<BoundedContext> boundedContext, ConsumerPerQueueChannelResolver channelResolver, IRequestResponseFactory factory, ISerializer serializer, IOptionsMonitor<RabbitMqConsumerOptions> consumerOptionsMonitor, IServiceProvider serviceProvider, ILogger<RpcEndpoint<TRequest, TResponse>> logger)
    {
        this.channelResolver = channelResolver;
        this.consumerOptions = consumerOptionsMonitor.CurrentValue;
        this.boundedContext = boundedContext.CurrentValue;
        this.options = options.CurrentValue;
        this.factory = factory;
        this.serializer = serializer;
        this.serviceProvider = serviceProvider;
        this.logger = logger;

        route = GetRoute();
    }

    public async Task<TResponse> SendAsync(TRequest request)
    {
        ResponseConsumer<TRequest, TResponse> current = client ?? await StartClientAsync().ConfigureAwait(false);

        TResponse response = new TResponse();

        if (current is null)
        {
            // StartClientAsync already logged the cause with its exception; hand the same reason to the caller instead of a NullReferenceException.
            response.Error = clientStartError ?? $"Unable to start RPC client for {route}.";
            return response;
        }

        try
        {
            response = await current.SendAsync(request).WaitAsync(TimeSpan.FromSeconds(consumerOptions.RpcTimeout)).ConfigureAwait(false);
        }
        catch (TimeoutException timedOutEx)
        {
            string error = "The server not responding for too long...";
            logger.LogError(timedOutEx, error);
            response.Error = error;
        }
        catch (Exception ex) when (True(() => logger.LogError(ex, ex.Message)))
        {
            response.Error = ex.Message;
        }

        return response;
    }

    async Task IRpc.StartServerAsync()
    {
        try
        {
            IRabbitMqOptions scopedOptions = options.GetOptionsFor(boundedContext.Name);

            for (int workerNumber = 0; workerNumber < consumerOptions.RpcWorkersCount; workerNumber++)
            {
                string workerChannelKey = $"{route}_{workerNumber}";
                string connectionKey = scopedOptions.GetConnectionKey(ConnectionResolver.Consume);

                IChannel requestChannel = await channelResolver.ResolveAsync(workerChannelKey, scopedOptions, options.VHost, connectionKey).ConfigureAwait(false);

                var stoppingToken = serviceProvider.GetService<IHostApplicationLifetime>()?.ApplicationStopping ?? CancellationToken.None;
                var server = new RequestConsumer<TRequest, TResponse>(route, requestChannel, factory, serializer, serviceProvider, logger, stoppingToken);
                servers.Add(server);
                await server.StartAsync().ConfigureAwait(false);
            }

            if (logger.IsEnabled(LogLevel.Information))
                logger.LogInformation("{rmqrpcworkerscount} RPC request consumers started for {route}.", consumerOptions.RpcWorkersCount, route);
        }
        catch (Exception ex) when (True(() => logger.LogError(ex, "Unable to start rpc server for {rmqroute}.", route))) { }
    }

    async Task IRpc.StopConsumersAsync(CancellationToken cancellationToken)
    {
        var tasks = servers.Select(server => server.StopAsync(cancellationToken)).ToList();
        if (client is not null)
            tasks.Add(client.StopAsync(cancellationToken));

        await Task.WhenAll(tasks).ConfigureAwait(false);
        servers.Clear();
        client = null;
    }

    private static SemaphoreSlim threadGate = new SemaphoreSlim(1, 1);

    private async Task<ResponseConsumer<TRequest, TResponse>> StartClientAsync()
    {
        try
        {
            await threadGate.WaitAsync(20000).ConfigureAwait(false);

            if (client is null)
            {
                var attributes = typeof(TRequest).GetCustomAttributes(typeof(DataContractAttribute), false);
                var dataContractAttribute = attributes[0] as DataContractAttribute;
                string destinationBC = dataContractAttribute.Namespace;

                if (destinationBC is not null)
                {
                    var cfgFound = options.ExternalServices?.Where(opt => opt.BoundedContext.Equals(destinationBC, System.StringComparison.OrdinalIgnoreCase)).Any();
                    if (cfgFound.HasValue && cfgFound.Value)
                    {
                        IRabbitMqOptions scopedOptions = options.GetOptionsFor(destinationBC);
                        string connectionKey = scopedOptions.GetConnectionKey(ConnectionResolver.Consume);

                        IChannel requestChannel = await channelResolver.ResolveAsync(route, scopedOptions, destinationBC, connectionKey).ConfigureAwait(false);
                        client = new ResponseConsumer<TRequest, TResponse>(route, requestChannel, serializer, logger);
                        await client.StartAsync().ConfigureAwait(false);

                        if (logger.IsEnabled(LogLevel.Information))
                            logger.LogInformation("{rmqrpcworkerscount} RPC response consumers started for {route}.", consumerOptions.RpcWorkersCount, route);
                    }
                    else
                    {
                        throw new Exception($"There is a missing configuration Inception:Transport:RabbitMQ:ExternalServices. Destination BC is {destinationBC}.");
                    }
                }
                else
                {
                    throw new Exception($"Missing Namespace in DataContract for {typeof(TRequest).Name}");
                }
            }
        }
        catch (Exception ex) when (False(() => logger.LogError(ex, "Unable to start RPC client for {rmqroute}. {reason}", route, ex.Message))) { }
        catch (Exception ex) { clientStartError = $"Unable to start RPC client for {route}. {ex.Message}"; }
        finally
        {
            threadGate?.Release();
        }

        return client;
    }

    private string GetRoute() => $"{typeof(TRequest).Name}s";
}
