using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Machine.Specifications;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;

namespace One.Inception.Transport.RabbitMQ.Tests;

[Subject("Consumer lifecycle")]
public class When_a_consumer_is_restarted
{
    It should_keep_subscriptions_and_create_a_fresh_cancellation_source = () => VerifyAsync().GetAwaiter().GetResult();

    static async Task VerifyAsync()
    {
        var subscriber = new StubSubscriber();
        var resolver = new StubChannelResolver();
        var consumer = LifecycleTest.Create(resolver, subscriber);
        await consumer.StartAsync();
        await consumer.StartAsync();
        resolver.Channels.Count.ShouldEqual(1);

        Task first = ConsumerTest.DeliverAsync(resolver.Channels[0].Consumer);
        await subscriber.Started.Task.WaitAsync(TimeSpan.FromSeconds(5));
        CancellationToken firstToken = subscriber.Token;
        await consumer.StopAsync().WaitAsync(TimeSpan.FromSeconds(5));
        await first.WaitAsync(TimeSpan.FromSeconds(5));

        await consumer.StartAsync();
        resolver.Channels.Count.ShouldEqual(2);
        Task second = ConsumerTest.DeliverAsync(resolver.Channels[1].Consumer);
        subscriber.Token.IsCancellationRequested.ShouldBeFalse();
        subscriber.Token.Equals(firstToken).ShouldBeFalse();

        await consumer.StopAsync().WaitAsync(TimeSpan.FromSeconds(5));
        await second.WaitAsync(TimeSpan.FromSeconds(5));
        await consumer.StopAsync();
    }
}

[Subject("Consumer lifecycle")]
public class When_application_shutdown_is_requested
{
    It should_notify_the_handler_before_explicit_consumer_stop = () => VerifyAsync().GetAwaiter().GetResult();

    static async Task VerifyAsync()
    {
        using var lifetime = new StubLifetime();
        var subscriber = new StubSubscriber();
        var resolver = new StubChannelResolver();
        var consumer = LifecycleTest.Create(resolver, subscriber, lifetime);
        await consumer.StartAsync();
        Task delivery = ConsumerTest.DeliverAsync(resolver.Channels[0].Consumer);
        await subscriber.Started.Task.WaitAsync(TimeSpan.FromSeconds(5));

        lifetime.StopApplication();
        await delivery.WaitAsync(TimeSpan.FromSeconds(5));
        subscriber.Token.IsCancellationRequested.ShouldBeTrue();
        resolver.Channels[0].Acknowledgements.ShouldEqual(0);
        await consumer.StopAsync();
    }
}

[Subject("Consumer lifecycle")]
public class When_consumer_startup_fails
{
    It should_close_partial_consumers_and_allow_a_fresh_start = () => VerifyAsync().GetAwaiter().GetResult();

    static async Task VerifyAsync()
    {
        var resolver = new StubChannelResolver { FailNextConsume = true };
        var subscriber = new StubSubscriber();
        var consumer = LifecycleTest.Create(resolver, subscriber);
        Exception failure = null;
        try { await consumer.StartAsync(); }
        catch (Exception ex) { failure = ex; }
        failure.ShouldBeNull(); // startup failure is logged, not thrown, so host startup continues
        resolver.Channels[0].Closed.ShouldBeTrue();

        await consumer.StartAsync();
        resolver.Channels.Count.ShouldEqual(2);
        Task delivery = ConsumerTest.DeliverAsync(resolver.Channels[1].Consumer);
        subscriber.Token.IsCancellationRequested.ShouldBeFalse();
        await consumer.StopAsync().WaitAsync(TimeSpan.FromSeconds(5));
        await delivery.WaitAsync(TimeSpan.FromSeconds(5));
    }
}

public static class LifecycleTest
{
    public static Consumer<IApplicationService> Create(StubChannelResolver resolver, StubSubscriber subscriber, IHostApplicationLifetime lifetime = null, ILogger<Consumer<IApplicationService>> logger = null, IOptionsMonitor<InceptionHostOptions> hostOptions = null)
    {
        var context = new StubOptions<BoundedContext>(new BoundedContext { Name = "test" });
        var factory = new ConsumerFactory<IApplicationService>(
            new TypeContainer<IProcessManager>(Array.Empty<Type>()),
            new TypeContainer<ITrigger>(Array.Empty<Type>()),
            new StubOptions<RabbitMqOptions>(new RabbitMqOptions()),
            resolver,
            new StubOptions<RabbitMqConsumerOptions>(new RabbitMqConsumerOptions { WorkersCount = 1 }),
            context,
            new StubSerializer(),
            new StubSubscribers<IApplicationService>(new[] { subscriber }),
            null,
            new BoundedContextRabbitMqNamer(context),
            NullLogger<ConsumerFactory<IApplicationService>>.Instance);
        return new Consumer<IApplicationService>(
            new StubSubscribers<IApplicationService>(new[] { subscriber }), new StubSerializer(), factory,
            logger ?? NullLogger<Consumer<IApplicationService>>.Instance, lifetime, hostOptions);
    }
}

public class StubChannelResolver : ConsumerPerQueueChannelResolver
{
    public List<ChannelStub> Channels = new();
    public bool FailNextConsume;

    public StubChannelResolver() : base(null) { }

    public override Task<IChannel> ResolveAsync(string resolveKey, IRabbitMqOptions options, string boundedContext, string connectionKey)
    {
        var channel = ChannelStub.Create();
        channel.FailConsume = FailNextConsume;
        FailNextConsume = false;
        Channels.Add(channel);
        return Task.FromResult(channel.Channel);
    }
}

public class StubOptions<T> : IOptionsMonitor<T>
{
    public StubOptions(T value) { CurrentValue = value; }
    public T CurrentValue { get; }
    public T Get(string name) => CurrentValue;
    public IDisposable OnChange(Action<T, string> listener) => null;
}

public class StubLifetime : IHostApplicationLifetime, IDisposable
{
    private readonly CancellationTokenSource stopping = new();
    public CancellationToken ApplicationStarted => CancellationToken.None;
    public CancellationToken ApplicationStopping => stopping.Token;
    public CancellationToken ApplicationStopped => CancellationToken.None;
    public void StopApplication() => stopping.Cancel();
    public void Dispose() => stopping.Dispose();
}
