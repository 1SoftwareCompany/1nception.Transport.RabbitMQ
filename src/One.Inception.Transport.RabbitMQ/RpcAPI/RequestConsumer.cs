using System;
using System.Threading.Tasks;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.DependencyInjection;

namespace One.Inception.Transport.RabbitMQ.RpcAPI;

public class RequestConsumer<TRequest, TResponse> : AsyncConsumerBase
    where TRequest : IRpcRequest<TResponse>
    where TResponse : IRpcResponse, new()
{
    private string queue;
    private readonly IRequestResponseFactory factory;
    private readonly IServiceProvider serviceProvider;
    private static string _timeout = "30000";

    public RequestConsumer(string queue, IChannel channel, IRequestResponseFactory factory, ISerializer serializer, IServiceProvider serviceProvider, ILogger logger)
      : base(channel, serializer, logger)
    {
        this.queue = queue;
        this.factory = factory;
        this.serviceProvider = serviceProvider;
    }

    public override async Task StartAsync()
    {
        await channel.QueueDeclareAsync(queue, exclusive: false).ConfigureAwait(false);
        await channel.BasicQosAsync(0, 1, false).ConfigureAwait(false);
        await channel.BasicConsumeAsync(queue, autoAck: false, this).ConfigureAwait(false);
    }

    protected override async Task DeliverMessageToSubscribersAsync(BasicDeliverEventArgs ev, AsyncEventingBasicConsumer consumer)
    {
        using (IServiceScope scope = serviceProvider.CreateScope())
        {
            TRequest request = default;
            RpcResponseTransmission response = default;

            try // Proccess request and publish response
            {
                request = serializer.DeserializeFromBytes<TRequest>(ev.Body.ToArray());

                IRequestHandler<TRequest, TResponse> handler = factory.CreateHandler<TRequest, TResponse>(request.Tenant, scope.ServiceProvider);
                TResponse handlerResponse = await handler.HandleAsync(request).ConfigureAwait(false);
                response = new RpcResponseTransmission(handlerResponse);
            }
            catch (Exception ex) when (True(() => logger.LogError(ex, "Request listened on {rmqqueue} failed.", queue)))
            {
                response = RpcResponseTransmission.WithError(ex);
            }
            finally
            {
                BasicProperties replyProps = new BasicProperties();
                replyProps.CorrelationId = ev.BasicProperties.CorrelationId; // correlate requests with the responses
                replyProps.ReplyTo = ev.BasicProperties.ReplyTo;
                replyProps.Persistent = false;
                replyProps.Expiration = _timeout;

                byte[] responseBytes = serializer.SerializeToBytes(response);
                await channel.BasicPublishAsync("", routingKey: replyProps.ReplyTo, false, replyProps, responseBytes).ConfigureAwait(false);
                await channel.BasicAckAsync(ev.DeliveryTag, false).ConfigureAwait(false);
            }
        }
    }
}
