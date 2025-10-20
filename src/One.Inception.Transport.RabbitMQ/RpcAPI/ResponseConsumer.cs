using System;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Threading.Tasks;
using System.Collections.Concurrent;
using System.Threading;
using System.Collections.Generic;

namespace One.Inception.Transport.RabbitMQ.RpcAPI;

public class ResponseConsumer<TRequest, TResponse> : AsyncConsumerBase
    where TRequest : IRpcRequest<TResponse>
    where TResponse : IRpcResponse, new()
{
    private readonly ConcurrentDictionary<string, TaskCompletionSource<TResponse>> requestTracker = new ConcurrentDictionary<string, TaskCompletionSource<TResponse>>();
    private static HashSet<string> occupiedNames = new HashSet<string>();
    private static string _timeout = "30000";
    private readonly string queueName;
    private string queueToConsume;

    public ResponseConsumer(string queueName, IChannel channel, ISerializer serializer, ILogger logger)
      : base(channel, serializer, logger)
    {
        this.queueName = queueName;
    }

    public override async Task StartAsync()
    {
        queueToConsume = await DeclareUniqueQueueAsync().ConfigureAwait(false);
        await channel.BasicConsumeAsync(queue: queueToConsume, autoAck: true, consumer: this).ConfigureAwait(false);

        if (logger.IsEnabled(LogLevel.Information))
            logger.LogInformation("RPC response consumer started for {rmqqueue}.", queueToConsume);
    }

    public Task<TResponse> SendAsync(TRequest request, CancellationToken cancellationToken = default(CancellationToken))
    {
        string correlationId = Guid.NewGuid().ToString(); // Create a new request id
        BasicProperties props = new BasicProperties();
        props.CorrelationId = correlationId;
        props.ReplyTo = queueToConsume;
        props.Expiration = _timeout;
        props.Persistent = false;

        byte[] messageBytes = serializer.SerializeToBytes(request);

        TaskCompletionSource<TResponse> taskSource = new TaskCompletionSource<TResponse>();
        requestTracker.TryAdd(correlationId, taskSource);

        channel.BasicPublish(exchange: "", routingKey: queueName, basicProperties: props, body: messageBytes); // publish request

        if (logger.IsEnabled(LogLevel.Debug))
            logger.LogDebug("Publish requests, to {rmqqueue}", queueName);

        cancellationToken.Register(() => requestTracker.TryRemove(correlationId, out _));
        return taskSource.Task;
    }

    protected override Task DeliverMessageToSubscribersAsync(BasicDeliverEventArgs ev, AsyncEventingBasicConsumer consumer) // await responses and add to collection
    {
        RpcResponseTransmission transient = new RpcResponseTransmission();
        TaskCompletionSource<TResponse> task = default;
        TResponse response = new TResponse();

        try
        {
            if (requestTracker.TryRemove(ev.BasicProperties.CorrelationId, out task) == false) // confirm that request is not proccessed
            {
                return Task.CompletedTask;
            }

            transient = serializer.DeserializeFromBytes<RpcResponseTransmission>(ev.Body.ToArray());
            if (transient is null)
                throw new Exception("Failed to deserialize.");

            response.Data = transient.Data;
            response.Error = transient.Error;

            task.TrySetResult(response);

            return Task.CompletedTask;
        }
        catch (Exception ex) when (False(() => logger.LogError(ex, "Unable to process response!"))) { }
        catch (Exception)
        {
            response.Data = transient.Data;
            response.Error = transient.Error;
            task?.TrySetResult(response);
        }

        return Task.CompletedTask;
    }

    private async Task<string> DeclareUniqueQueueAsync()
    {
        string queue = $"{queueName}.client.{Guid.NewGuid().ToString().Substring(0, 8)}";
        var queueDeclareResult = await channel.QueueDeclareAsync(queue, exclusive: false);

        return queueDeclareResult.QueueName;
    }
}
