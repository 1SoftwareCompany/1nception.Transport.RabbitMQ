using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Machine.Specifications;
using Microsoft.Extensions.Logging;

namespace One.Inception.Transport.RabbitMQ.Tests;

[Subject("Shutdown timeout")]
public class When_the_shutdown_deadline_expires
{
    It should_use_the_configured_fallback_or_the_callers_deadline_and_warn = () =>
        Task.WhenAll(VerifyAsync(false), VerifyAsync(true), VerifyLifecycleAsync(false), VerifyLifecycleAsync(true)).GetAwaiter().GetResult();

    static async Task VerifyAsync(bool dedicated)
    {
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var subscriber = new StubSubscriber(_ => release.Task);
        var channel = ChannelStub.Create();
        var logger = new WarningLogger<AsyncConsumerBase>();
        AsyncConsumerBase consumer = dedicated
            ? new AsyncConsumerForSingleSubscriber("test", channel.Channel, subscriber, new StubSerializer(), logger)
            : new AsyncConsumer<object>("test", channel.Channel, new StubSubscribers<object>(new[] { subscriber }), new StubSerializer(), logger);
        Task delivery = ConsumerTest.DeliverAsync(consumer);
        using var deadline = new CancellationTokenSource(TimeSpan.FromMilliseconds(100));
        try
        {
            await consumer.StopAsync(deadline.Token).WaitAsync(TimeSpan.FromSeconds(5));
            logger.Warnings.Any(message => message.Contains("shutdown deadline expired")).ShouldBeTrue();
            delivery.IsCompleted.ShouldBeFalse();
            channel.Closed.ShouldBeTrue();
            channel.Acknowledgements.ShouldEqual(0);
        }
        finally
        {
            release.TrySetResult();
            await delivery.WaitAsync(TimeSpan.FromSeconds(5));
        }
    }

    static async Task VerifyLifecycleAsync(bool withCallerDeadline)
    {
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var subscriber = new StubSubscriber(_ => release.Task);
        var resolver = new StubChannelResolver();
        var logger = new WarningLogger<Consumer<IApplicationService>>();
        var options = new StubOptions<InceptionHostOptions>(new InceptionHostOptions { ShutdownTimeout = TimeSpan.FromMilliseconds(100) });
        var consumer = LifecycleTest.Create(resolver, subscriber, logger: logger, hostOptions: options);
        await consumer.StartAsync();
        Task delivery = ConsumerTest.DeliverAsync(resolver.Channels[0].Consumer);
        using var caller = new CancellationTokenSource();
        try
        {
            Task stopping = consumer.StopAsync(withCallerDeadline ? caller.Token : default);
            if (withCallerDeadline)
            {
                await Task.Delay(200);
                stopping.IsCompleted.ShouldBeFalse();
                caller.Cancel();
            }
            await stopping.WaitAsync(TimeSpan.FromSeconds(5));
            logger.Warnings.Any(message => message.Contains("shutdown deadline expired")).ShouldBeTrue();
            await resolver.Channels[0].Consumer.StopAsync().WaitAsync(TimeSpan.FromSeconds(5));
            resolver.Channels[0].Acknowledgements.ShouldEqual(0);
        }
        finally
        {
            caller.Cancel();
            release.TrySetResult();
            await delivery.WaitAsync(TimeSpan.FromSeconds(5));
        }
    }
}

[Subject("Shutdown timeout")]
public class When_shutdown_finishes_before_the_timeout
{
    It should_not_warn = () =>
    {
        var channel = ChannelStub.Create();
        var logger = new WarningLogger<AsyncConsumerBase>();
        var subscriber = new StubSubscriber();
        var consumer = new AsyncConsumerForSingleSubscriber("test", channel.Channel, subscriber, new StubSerializer(), logger);
        consumer.StopAsync().WaitAsync(TimeSpan.FromSeconds(5)).GetAwaiter().GetResult();
        logger.Warnings.Count.ShouldEqual(0);
    };
}

public class WarningLogger<T> : ILogger<T>
{
    public ConcurrentQueue<string> Warnings { get; } = new();
    public IDisposable BeginScope<TState>(TState state) => null;
    public bool IsEnabled(LogLevel logLevel) => true;
    public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception exception, Func<TState, Exception, string> formatter)
    {
        if (logLevel == LogLevel.Warning)
            Warnings.Enqueue(formatter(state, exception));
    }
}
