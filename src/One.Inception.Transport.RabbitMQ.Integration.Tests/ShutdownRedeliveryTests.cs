using System;
using System.Threading;
using System.Threading.Tasks;
using Machine.Specifications;
using Microsoft.Extensions.Logging.Abstractions;
using One.Inception.Transport.RabbitMQ.Tests;
using RabbitMQ.Client;

namespace One.Inception.Transport.RabbitMQ.Integration.Tests;

[Subject("RabbitMQ shutdown and redelivery")]
public class When_rabbitmq_handlers_are_cancelled
{
    It should_redeliver_unfinished_shared_and_dedicated_work = () =>
    {
        VerifyAsync(false, false, false).GetAwaiter().GetResult();
        VerifyAsync(true, false, false).GetAwaiter().GetResult();
    };

    It should_redeliver_work_when_another_subscriber_faulted = () =>
        VerifyAsync(false, false, true).GetAwaiter().GetResult();

    It should_acknowledge_a_handler_that_finishes_during_shutdown = () =>
        VerifyAsync(true, true, false).GetAwaiter().GetResult();

    It should_close_the_channel_at_the_deadline_even_if_the_handler_ignores_cancellation = () =>
        VerifyAsync(true, false, false, ignoreCancellation: true).GetAwaiter().GetResult();

    static async Task VerifyAsync(bool dedicated, bool complete, bool mixedFailure, bool ignoreCancellation = false)
    {
        var factory = new ConnectionFactory
        {
            HostName = "127.0.0.1",
            Port = int.Parse(Environment.GetEnvironmentVariable("INCEPTION_RABBITMQ_TEST_PORT") ?? "5679"),
            AutomaticRecoveryEnabled = false
        };
        await using var connection = await factory.CreateConnectionAsync();
        await using var channel = await connection.CreateChannelAsync();
        await using var observerConnection = await factory.CreateConnectionAsync();
        await using var observer = await observerConnection.CreateChannelAsync();
        string queue = "inception-cancellation-" + Guid.NewGuid().ToString("N");
        // durable, like the queues RabbitMqStartup declares: RabbitMQ 4.1+ rejects transient non-exclusive
        // queues (AMQP 541, feature 'transient_nonexcl_queues'). Redelivery of unacked work is unaffected.
        await channel.QueueDeclareAsync(queue, durable: true, exclusive: false, autoDelete: false);

        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var subscriber = new StubSubscriber(async token =>
        {
            if (complete)
            {
                using var registration = token.Register(() => release.TrySetResult());
                await release.Task;
            }
            else if (ignoreCancellation)
                await release.Task;
            else
                await Task.Delay(Timeout.Infinite, token);
        });
        var subscribers = mixedFailure
            ? new[] { subscriber, new StubSubscriber(_ => Task.FromException(new InvalidOperationException("test failure"))) }
            : new[] { subscriber };
        AsyncConsumerBase consumer = dedicated
            ? new AsyncConsumerForSingleSubscriber(queue, channel, subscriber, new StubSerializer(), NullLogger.Instance)
            : new AsyncConsumer<object>(queue, channel, new StubSubscribers<object>(subscribers), new StubSerializer(), NullLogger.Instance);

        try
        {
            await consumer.StartAsync();
            await observer.BasicPublishAsync("", queue, false, new BasicProperties(), new byte[] { 1 });
            await subscriber.Started.Task.WaitAsync(TimeSpan.FromSeconds(5));

            using var deadline = new CancellationTokenSource(ignoreCancellation
                ? TimeSpan.FromMilliseconds(100) : TimeSpan.FromSeconds(5));
            await consumer.StopAsync(deadline.Token).WaitAsync(TimeSpan.FromSeconds(7));

            if (complete)
            {
                (await observer.BasicGetAsync(queue, autoAck: false)).ShouldBeNull();
            }
            else
            {
                using var redeliveryTimeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));
                BasicGetResult redelivery;
                do
                {
                    redelivery = await observer.BasicGetAsync(queue, autoAck: false, redeliveryTimeout.Token);
                    if (redelivery is null)
                        await Task.Delay(20, redeliveryTimeout.Token);
                } while (redelivery is null);

                redelivery.Redelivered.ShouldBeTrue();
                await observer.BasicAckAsync(redelivery.DeliveryTag, false);
            }
        }
        finally
        {
            release.TrySetResult();
            await consumer.StopAsync();
            await observer.QueueDeleteAsync(queue);
        }
    }
}
