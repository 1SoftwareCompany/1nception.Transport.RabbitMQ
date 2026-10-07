using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace One.Inception.Transport.RabbitMQ.Startup;

public class SchedulePoker<T> //where T : IMessageHandler
{
    private readonly IOptionsMonitor<RabbitMqOptions> rmqOptionsMonitor;
    private readonly ConnectionResolver connectionResolver;

    public SchedulePoker(IOptionsMonitor<RabbitMqOptions> rmqOptionsMonitor, ConnectionResolver connectionResolver)
    {
        this.rmqOptionsMonitor = rmqOptionsMonitor;
        this.connectionResolver = connectionResolver;
    }

    public async Task PokeAsync(string queueName, CancellationToken cancellationToken)
    {
        while (cancellationToken.IsCancellationRequested == false)
        {
            try
            {
                string connectionKey = rmqOptionsMonitor.CurrentValue.GetConnectionKey(ConnectionResolver.Consume);
                IConnection connection = await connectionResolver.ResolveAsync(rmqOptionsMonitor.CurrentValue, connectionKey, cancellationToken).ConfigureAwait(false);

                await using IChannel channel = await connection.CreateChannelAsync(cancellationToken: cancellationToken).ConfigureAwait(false);

                var consumer = new AsyncEventingBasicConsumer(channel);
                consumer.ReceivedAsync += AsyncListener_Received;
                await channel.BasicConsumeAsync(queueName, autoAck: false, consumer: consumer, cancellationToken).ConfigureAwait(false);

                await Task.Delay(30000, cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                try
                {
                    await Task.Delay(5000, cancellationToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
            }
        }
    }

    private Task AsyncListener_Received(object sender, BasicDeliverEventArgs @event)
    {
        return Task.CompletedTask;
    }
}
