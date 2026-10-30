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

public static class ConsumerTest
{
    public static AsyncConsumerBase Create(IChannel channel, bool dedicated, params ISubscriber[] subscribers)
        => Create(channel, dedicated, default, subscribers);

    public static AsyncConsumerBase Create(IChannel channel, bool dedicated, CancellationToken token, params ISubscriber[] subscribers)
        => dedicated
            ? new AsyncConsumerForSingleSubscriber("test", channel, subscribers.Single(), new StubSerializer(), NullLogger.Instance, token)
            : new AsyncConsumer<object>("test", channel, new StubSubscribers<object>(subscribers), new StubSerializer(), NullLogger.Instance, token);

    public static Task DeliverAsync(AsyncConsumerBase consumer)
        => consumer.HandleBasicDeliverAsync("test", 1, false, "", "", new BasicProperties(), Array.Empty<byte>());
}

public class StubSubscriber : ISubscriber
{
    private readonly Func<CancellationToken, Task> process;
    public TaskCompletionSource Started = new(TaskCreationOptions.RunContinuationsAsynchronously);
    public CancellationToken Token;
    public string Id { get; } = Guid.NewGuid().ToString();
    public Type HandlerType => typeof(StubSubscriber);

    public StubSubscriber(Func<CancellationToken, Task> process = null)
    {
        this.process = process ?? (token => Task.Delay(Timeout.Infinite, token));
    }

    public IEnumerable<Type> GetInvolvedMessageTypes() => new[] { typeof(TestSignal) };

    public Task ProcessAsync(InceptionMessage message, CancellationToken cancellationToken = default)
    {
        Token = cancellationToken;
        Started.TrySetResult();
        return process(cancellationToken);
    }
}

public class StubSubscribers<T> : ISubscriberCollection<T>
{
    public StubSubscribers(IEnumerable<ISubscriber> subscribers) { Subscribers = subscribers; }
    public IEnumerable<ISubscriber> Subscribers { get; }
    public IEnumerable<ISubscriber> GetInterestedSubscribers(InceptionMessage message) => Subscribers;
    public void Subscribe(ISubscriber subscriber) => throw new NotSupportedException();
    public void UnsubscribeAll() => throw new NotSupportedException();
}

public class TestSignal : ISignal
{
    public DateTimeOffset Timestamp => DateTimeOffset.UtcNow;
}

public class StubSerializer : ISerializer
{
    public byte[] SerializeToBytes<T>(T message) => Array.Empty<byte>();
    public string SerializeToString<T>(T message) => "";
    public T DeserializeFromBytes<T>(byte[] bytes)
        => (T)(object)new InceptionMessage(new TestSignal(), new Dictionary<string, string>());
}

public class ChannelStub : DispatchProxy
{
    public IChannel Channel;
    public bool Closed;
    public int Acknowledgements;
    public int CloseCount;
    public int Publishes;
    public AsyncConsumerBase Consumer;
    public bool FailConsume;

    public static ChannelStub Create()
    {
        IChannel channel = Create<IChannel, ChannelStub>();
        var stub = (ChannelStub)channel;
        stub.Channel = channel;
        return stub;
    }

    protected override object Invoke(MethodInfo method, object[] args)
    {
        switch (method.Name)
        {
            case "get_IsOpen": return !Closed;
            case "get_IsClosed": return Closed;
            case "BasicConsumeAsync":
                if (FailConsume) throw new InvalidOperationException("test startup failure");
                Consumer = args.OfType<AsyncConsumerBase>().Single();
                return Task.FromResult("test");
            case "BasicPublishAsync": Interlocked.Increment(ref Publishes); break;
            case "BasicAckAsync": Interlocked.Increment(ref Acknowledgements); break;
            case "CloseAsync":
                Closed = true;
                Interlocked.Increment(ref CloseCount);
                break;
        }

        if (method.ReturnType == typeof(Task)) return Task.CompletedTask;
        if (method.ReturnType.IsGenericType && method.ReturnType.GetGenericTypeDefinition() == typeof(Task<>))
        {
            Type resultType = method.ReturnType.GetGenericArguments()[0];
            object result = resultType.IsValueType ? Activator.CreateInstance(resultType) : null;
            return typeof(Task).GetMethod(nameof(Task.FromResult)).MakeGenericMethod(resultType).Invoke(null, new[] { result });
        }
        if (method.ReturnType.IsValueType) return Activator.CreateInstance(method.ReturnType);
        return null;
    }
}
