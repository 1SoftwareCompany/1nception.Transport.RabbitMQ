using System;
using System.Linq;
using System.Runtime.Serialization;
using One.Inception.Hosting;
using System.Threading;
using System.Threading.Tasks;
using Machine.Specifications;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using One.Inception.Transport.RabbitMQ.RpcAPI;

namespace One.Inception.Transport.RabbitMQ.Tests;

[Subject("RPC cancellation")]
public class When_rpc_handlers_stop
{
    It should_not_publish_a_response_or_acknowledge_cancelled_requests = () => VerifyAsync(false).GetAwaiter().GetResult();
    It should_publish_and_acknowledge_requests_completed_during_shutdown = () => VerifyAsync(true).GetAwaiter().GetResult();

    static async Task VerifyAsync(bool complete)
    {
        using var services = new ServiceCollection().BuildServiceProvider();
        var channel = ChannelStub.Create();
        var factory = new StubRpcFactory(complete);
        var consumer = new RequestConsumer<StubRpcRequest, RpcResponse<StubRpcPayload>>(
            "test", channel.Channel, factory, new StubRpcSerializer(), services, NullLogger.Instance);
        Task delivery = ConsumerTest.DeliverAsync(consumer);
        await factory.Started.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await consumer.StopAsync().WaitAsync(TimeSpan.FromSeconds(5));
        await delivery.WaitAsync(TimeSpan.FromSeconds(5));
        factory.Token.IsCancellationRequested.ShouldBeTrue();
        channel.Acknowledgements.ShouldEqual(complete ? 1 : 0);
        channel.Publishes.ShouldEqual(complete ? 1 : 0);
    }
}

[Subject("RPC cancellation")]
public class When_the_rpc_host_stops
{
    It should_cancel_and_stop_every_worker_not_only_the_last = () => VerifyAsync(false).GetAwaiter().GetResult();
    It should_use_the_configured_timeout_and_warn_for_uncooperative_requests = () => VerifyAsync(true).GetAwaiter().GetResult();

    static async Task VerifyAsync(bool uncooperative)
    {
        var resolver = new StubChannelResolver();
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var factory = new StubRpcFactory(false, uncooperative ? release.Task : null);
        var context = new StubOptions<BoundedContext>(new BoundedContext { Name = "test" });
        var rabbitOptions = new StubOptions<RabbitMqOptions>(new RabbitMqOptions());
        var consumerOptions = new StubOptions<RabbitMqConsumerOptions>(new RabbitMqConsumerOptions { RpcWorkersCount = 2 });
        var options = new StubOptions<InceptionHostOptions>(new InceptionHostOptions { RpcApiEnabled = true, ShutdownTimeout = TimeSpan.FromMilliseconds(100) });
        var services = new ServiceCollection();
        services.AddSingleton<IRpc<StubRpcRequest, RpcResponse<StubRpcPayload>>>(provider =>
            new RpcEndpoint<StubRpcRequest, RpcResponse<StubRpcPayload>>(
                rabbitOptions, context, resolver, factory, new StubRpcSerializer(),
                consumerOptions, provider, NullLogger<RpcEndpoint<StubRpcRequest, RpcResponse<StubRpcPayload>>>.Instance));
        using var provider = services.BuildServiceProvider();
        var logger = new WarningLogger<RpcHost>();
        var host = new RpcHost(provider, options, logger, factory);
        await host.StartAsync();
        resolver.Channels.Count.ShouldEqual(2);

        Task[] deliveries = resolver.Channels.Select(c => ConsumerTest.DeliverAsync(c.Consumer)).ToArray();
        try
        {
            await host.StopAsync().WaitAsync(TimeSpan.FromSeconds(5));
            logger.Warnings.Any(message => message.Contains("RPC shutdown deadline expired")).ShouldEqual(uncooperative);
            foreach (ChannelStub channel in resolver.Channels)
            {
                await channel.Consumer.StopAsync().WaitAsync(TimeSpan.FromSeconds(5));
                channel.Closed.ShouldBeTrue();
                channel.Acknowledgements.ShouldEqual(0);
            }
        }
        finally
        {
            release.TrySetResult();
            await Task.WhenAll(deliveries).WaitAsync(TimeSpan.FromSeconds(5));
        }
    }
}

[Subject("RPC client")]
public class When_the_rpc_client_has_no_external_services_config
{
    It should_return_the_missing_config_reason_instead_of_a_null_reference = () => VerifyAsync().GetAwaiter().GetResult();

    static async Task VerifyAsync()
    {
        using var provider = new ServiceCollection().BuildServiceProvider();
        var endpoint = new RpcEndpoint<StubRpcRequest, RpcResponse<StubRpcPayload>>(
            new StubOptions<RabbitMqOptions>(new RabbitMqOptions()), new StubOptions<BoundedContext>(new BoundedContext { Name = "test" }),
            new StubChannelResolver(), new StubRpcFactory(true), new StubRpcSerializer(),
            new StubOptions<RabbitMqConsumerOptions>(new RabbitMqConsumerOptions()), provider,
            NullLogger<RpcEndpoint<StubRpcRequest, RpcResponse<StubRpcPayload>>>.Instance);

        RpcResponse<StubRpcPayload> response = await endpoint.SendAsync(new StubRpcRequest()).WaitAsync(TimeSpan.FromSeconds(5));

        response.Error.ShouldContain("Inception:Transport:RabbitMQ:ExternalServices");
        response.Error.ShouldNotContain("Object reference");
    }
}

[DataContract(Name = "test-request", Namespace = "test")]
public class StubRpcRequest : RpcRequest<StubRpcPayload> { }

[DataContract(Name = "test-response", Namespace = "test")]
public class StubRpcPayload { }

public class StubRpcFactory : IRequestResponseFactory, IRequestHandler<StubRpcRequest, RpcResponse<StubRpcPayload>>
{
    private readonly bool complete;
    private readonly Task pending;
    public TaskCompletionSource Started = new(TaskCreationOptions.RunContinuationsAsynchronously);
    public CancellationToken Token;

    public StubRpcFactory(bool complete, Task pending = null) { this.complete = complete; this.pending = pending; }

    public ILookup<Type, Type> GetHandlers() => new[] { typeof(StubRpcRequest) }.ToLookup(_ => typeof(StubRpcFactory));
    public void RegisterHandlers(ILookup<Type, Type> handlers) => throw new NotSupportedException();

    public IRequestHandler<TRequest, TResponse> CreateHandler<TRequest, TResponse>(string tenant, IServiceProvider serviceProvider, CancellationToken cancellationToken = default)
        where TRequest : IRpcRequest<TResponse>
        => (IRequestHandler<TRequest, TResponse>)(object)this;

    public async Task<RpcResponse<StubRpcPayload>> HandleAsync(StubRpcRequest request, CancellationToken cancellationToken)
    {
        Token = cancellationToken;
        Started.TrySetResult();
        if (pending is not null)
            await pending;
        else if (complete)
        {
            var finish = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            using var registration = cancellationToken.Register(() => finish.TrySetResult());
            await finish.Task;
        }
        else
            await Task.Delay(Timeout.Infinite, cancellationToken);
        return RpcResponse<StubRpcPayload>.SetResult(new StubRpcPayload());
    }
}

public class StubRpcSerializer : ISerializer
{
    public byte[] SerializeToBytes<T>(T message) => Array.Empty<byte>();
    public string SerializeToString<T>(T message) => "";
    public T DeserializeFromBytes<T>(byte[] bytes) => (T)(object)new StubRpcRequest();
}
