using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Machine.Specifications;
using Microsoft.Extensions.Logging.Abstractions;
using One.Inception.MessageProcessing;
using RabbitMQ.Client;

namespace One.Inception.Transport.RabbitMQ.Tests;

[Subject("Consumer cancellation")]
public class When_consumers_stop
{
    It should_cancel_shared_and_dedicated_handlers_without_acknowledging = () => VerifyAsync().GetAwaiter().GetResult();

    static async Task VerifyAsync()
    {
        foreach (bool dedicated in new[] { false, true })
        {
            var subscriber = new StubSubscriber();
            var channel = ChannelStub.Create();
            var consumer = ConsumerTest.Create(channel.Channel, dedicated, subscriber);
            Task delivery = ConsumerTest.DeliverAsync(consumer);
            await subscriber.Started.Task.WaitAsync(TimeSpan.FromSeconds(5));
            await consumer.StopAsync().WaitAsync(TimeSpan.FromSeconds(5));
            await delivery.WaitAsync(TimeSpan.FromSeconds(5));

            subscriber.Token.IsCancellationRequested.ShouldBeTrue();
            channel.Acknowledgements.ShouldEqual(0);
            channel.Closed.ShouldBeTrue();
            await consumer.StopAsync();
            channel.CloseCount.ShouldEqual(1);
        }
    }

}

[Subject("Consumer cancellation")]
public class When_one_shared_subscriber_faults_and_another_is_cancelled
{
    It should_not_lose_the_cancelled_subscribers_work = () => VerifyAsync().GetAwaiter().GetResult();

    static async Task VerifyAsync()
    {
        var pending = new StubSubscriber();
        var failing = new StubSubscriber(_ => Task.FromException(new InvalidOperationException("failure")));
        var channel = ChannelStub.Create();
        var consumer = ConsumerTest.Create(channel.Channel, false, pending, failing);
        Task delivery = ConsumerTest.DeliverAsync(consumer);
        await pending.Started.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await consumer.StopAsync().WaitAsync(TimeSpan.FromSeconds(5));
        await delivery.WaitAsync(TimeSpan.FromSeconds(5));
        channel.Acknowledgements.ShouldEqual(0);
    }

}

[Subject("Consumer cancellation")]
public class When_a_handler_finishes_successfully_during_shutdown
{
    It should_acknowledge_completed_work_even_when_the_token_was_cancelled = () => VerifyAsync().GetAwaiter().GetResult();

    static async Task VerifyAsync()
    {
        var completed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var subscriber = new StubSubscriber(async token =>
        {
            using var registration = token.Register(() => completed.TrySetResult());
            await completed.Task;
        });
        var channel = ChannelStub.Create();
        var consumer = ConsumerTest.Create(channel.Channel, true, subscriber);
        Task delivery = ConsumerTest.DeliverAsync(consumer);
        await subscriber.Started.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await consumer.StopAsync().WaitAsync(TimeSpan.FromSeconds(5));
        await delivery.WaitAsync(TimeSpan.FromSeconds(5));
        channel.Acknowledgements.ShouldEqual(1);
    }

}

[Subject("Consumer cancellation")]
public class When_a_handler_ignores_cancellation
{
    It should_bound_shutdown_without_forcing_the_handler_to_finish = () => VerifyAsync().GetAwaiter().GetResult();

    static async Task VerifyAsync()
    {
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var subscriber = new StubSubscriber(_ => release.Task);
        var channel = ChannelStub.Create();
        var logger = new WarningLogger<AsyncConsumerBase>();
        var consumer = new AsyncConsumerForSingleSubscriber("test", channel.Channel, subscriber, new StubSerializer(), logger);
        Task delivery = ConsumerTest.DeliverAsync(consumer);
        await subscriber.Started.Task.WaitAsync(TimeSpan.FromSeconds(5));
        using var deadline = new CancellationTokenSource(TimeSpan.FromMilliseconds(100));

        await consumer.StopAsync(deadline.Token).WaitAsync(TimeSpan.FromSeconds(5));
        logger.Warnings.Any(message => message.Contains("shutdown deadline expired")).ShouldBeTrue();
        channel.Closed.ShouldBeTrue();
        channel.Acknowledgements.ShouldEqual(0);
        delivery.IsCompleted.ShouldBeFalse();

        release.SetResult();
        await delivery.WaitAsync(TimeSpan.FromSeconds(5));
        channel.Acknowledgements.ShouldEqual(0);
    }

}

[Subject("Consumer cancellation")]
public class When_shutdown_cancellation_is_thrown_synchronously
{
    It should_not_acknowledge_synchronous_cancellation = () => VerifyAsync().GetAwaiter().GetResult();

    static async Task VerifyAsync()
    {
        using var source = new CancellationTokenSource();
        var subscriber = new StubSubscriber(token =>
        {
            source.Cancel();
            token.ThrowIfCancellationRequested();
            return Task.CompletedTask;
        });
        var channel = ChannelStub.Create();
        var consumer = ConsumerTest.Create(channel.Channel, true, source.Token, subscriber);
        await ConsumerTest.DeliverAsync(consumer);
        await consumer.StopAsync();
        channel.Acknowledgements.ShouldEqual(0);
    }

}

[Subject("Consumer cancellation")]
public class When_a_local_operation_is_cancelled_without_shutdown
{
    It should_preserve_the_existing_ordinary_error_policy = () => VerifyAsync().GetAwaiter().GetResult();

    static async Task VerifyAsync()
    {
        var subscriber = new StubSubscriber(_ => Task.FromCanceled(new CancellationToken(true)));
        var channel = ChannelStub.Create();
        var consumer = ConsumerTest.Create(channel.Channel, true, subscriber);
        await ConsumerTest.DeliverAsync(consumer);
        channel.Acknowledgements.ShouldEqual(1);
        await consumer.StopAsync();
    }

}

[Subject("Consumer cancellation")]
public class When_a_buffered_delivery_arrives_after_shutdown
{
    It should_not_start_or_acknowledge_the_delivery = () => VerifyAsync().GetAwaiter().GetResult();

    static async Task VerifyAsync()
    {
        var subscriber = new StubSubscriber(_ => Task.CompletedTask);
        var channel = ChannelStub.Create();
        var consumer = ConsumerTest.Create(channel.Channel, true, subscriber);
        await consumer.StopAsync();
        await ConsumerTest.DeliverAsync(consumer);
        subscriber.Started.Task.IsCompleted.ShouldBeFalse();
        channel.Acknowledgements.ShouldEqual(0);
    }

}

[Subject("Consumer cancellation")]
public class When_two_delivery_callbacks_are_active_during_shutdown
{
    It should_wait_for_every_callback_not_just_the_first = () => VerifyAsync().GetAwaiter().GetResult();

    static async Task VerifyAsync()
    {
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        int calls = 0;
        var subscriber = new StubSubscriber(token =>
            Interlocked.Increment(ref calls) == 1 ? Task.Delay(Timeout.Infinite, token) : release.Task);
        var channel = ChannelStub.Create();
        var consumer = ConsumerTest.Create(channel.Channel, true, subscriber);
        Task first = ConsumerTest.DeliverAsync(consumer);
        Task second = ConsumerTest.DeliverAsync(consumer);
        Task stopping = consumer.StopAsync();
        await first.WaitAsync(TimeSpan.FromSeconds(5));
        stopping.IsCompleted.ShouldBeFalse();
        channel.Closed.ShouldBeFalse();
        release.SetResult();
        await Task.WhenAll(first, second, stopping).WaitAsync(TimeSpan.FromSeconds(5));
        channel.Acknowledgements.ShouldEqual(1);
    }

}

[Subject("Consumer cancellation")]
public class When_a_cancellation_callback_throws
{
    It should_still_drain_and_close_the_consumer = () => VerifyAsync().GetAwaiter().GetResult();

    static async Task VerifyAsync()
    {
        var subscriber = new StubSubscriber(async token =>
        {
            using var registration = token.Register(() => throw new InvalidOperationException("test callback failure"));
            await Task.Delay(Timeout.Infinite, token);
        });
        var channel = ChannelStub.Create();
        var consumer = ConsumerTest.Create(channel.Channel, true, subscriber);
        Task delivery = ConsumerTest.DeliverAsync(consumer);
        await subscriber.Started.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await consumer.StopAsync().WaitAsync(TimeSpan.FromSeconds(5));
        await delivery.WaitAsync(TimeSpan.FromSeconds(5));
        channel.Closed.ShouldBeTrue();
        channel.Acknowledgements.ShouldEqual(0);
    }
}
