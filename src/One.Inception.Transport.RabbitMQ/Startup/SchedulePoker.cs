using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Threading.Tasks;
using System.Threading;

namespace One.Inception.Transport.RabbitMQ.Startup;

public class SchedulePoker<T> //where T : IMessageHandler
{
    private readonly IOptionsMonitor<RabbitMqOptions> rmqOptionsMonitor;
    private readonly ConnectionResolver connectionResolver;
    private readonly IOptionsMonitor<BoundedContext> boundedContext;
    AsyncEventingBasicConsumer consumer;

    public SchedulePoker(IOptionsMonitor<RabbitMqOptions> rmqOptionsMonitor, ConnectionResolver connectionResolver, IOptionsMonitor<BoundedContext> boundedContext)
    {
        this.rmqOptionsMonitor = rmqOptionsMonitor;
        this.connectionResolver = connectionResolver;
        this.boundedContext = boundedContext;
    }

    public async Task PokeAsync(string queueName, CancellationToken cancellationToken)
    {
        try
        {
            while (cancellationToken.IsCancellationRequested == false)
            {
                IConnection connection = await connectionResolver.ResolveAsync(queueName, rmqOptionsMonitor.CurrentValue).ConfigureAwait(false);

                using (IChannel channel = await connection.CreateChannelAsync().ConfigureAwait(false))
                {
                    try
                    {
                        consumer = new AsyncEventingBasicConsumer(channel);
                        consumer.ReceivedAsync += AsyncListener_Received;

                        string consumerTag = await channel.BasicConsumeAsync(queue: queueName, autoAck: false, consumer: consumer).ConfigureAwait(false);

                        await Task.Delay(30000, cancellationToken).ConfigureAwait(false);

                        consumer.ReceivedAsync -= AsyncListener_Received;
                    }
                    catch (Exception)
                    {
                        await Task.Delay(5000).ConfigureAwait(false);
                    }
                }
            }
        }
        catch (Exception) { }
    }

    private Task AsyncListener_Received(object sender, BasicDeliverEventArgs @event)
    {
        return Task.CompletedTask;
    }
}

