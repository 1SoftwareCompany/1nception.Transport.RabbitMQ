﻿using System;
using System.Linq;
using System.Runtime.Serialization;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;

namespace One.Inception.Transport.RabbitMQ.RpcAPI;

public interface IRpc
{
    internal void StartServer();

    internal Task StopConsumersAsync();
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
    private RequestConsumer<TRequest, TResponse> server;
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
        client = client ?? await StartClientAsync().ConfigureAwait(false);

        TResponse response = new TResponse();

        try
        {
            response = await client.SendAsync(request).WaitAsync(TimeSpan.FromSeconds(consumerOptions.RpcTimeout)).ConfigureAwait(false);
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

    void IRpc.StartServer()
    {
        try
        {
            IRabbitMqOptions scopedOptions = options.GetOptionsFor(boundedContext.Name);
            IModel requestChannel = channelResolver.Resolve(route, scopedOptions, options.VHost);

            for (int workerNumber = 0; workerNumber < consumerOptions.RpcWorkersCount; workerNumber++)
                server = new RequestConsumer<TRequest, TResponse>(route, requestChannel, factory, serializer, serviceProvider, logger);

            if (logger.IsEnabled(LogLevel.Information))
                logger.LogInformation("{rmqrpcworkerscount} RPC request consumers started for {route}.", consumerOptions.RpcWorkersCount, route);
        }
        catch (Exception ex) when (True(() => logger.LogError(ex, "Unable to start rpc server for {rmqroute}.", route))) { }
    }

    async Task IRpc.StopConsumersAsync()
    {
        if (client is not null)
            await client.StopAsync().ConfigureAwait(false);

        if (server is not null)
            await server.StopAsync().ConfigureAwait(false);
    }

    private static SemaphoreSlim threadGate = new SemaphoreSlim(1, 1);
    private static bool isClientCreated = false;

    private async Task<ResponseConsumer<TRequest, TResponse>> StartClientAsync()
    {
        try
        {
            await threadGate.WaitAsync(20000).ConfigureAwait(false);

            if (isClientCreated == false)
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
                        IModel requestChannel = channelResolver.Resolve(route, scopedOptions, destinationBC);
                        client = new ResponseConsumer<TRequest, TResponse>(route, requestChannel, serializer, logger);
                        isClientCreated = true;
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
        catch (Exception ex) when (False(() => logger.LogError(ex, "Unable to start RPC client for {rmqroute} to destination BC.", route))) { }
        catch (Exception) { }
        finally
        {
            threadGate?.Release();
        }

        return client;
    }

    private string GetRoute() => $"{typeof(TRequest).Name}s";
}
