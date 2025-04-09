namespace One.Inception.Transport.RabbitMQ;

public class ConsumerPerQueueChannelResolver : ChannelResolverBase // channels per queue
{
    public ConsumerPerQueueChannelResolver(ConnectionResolver connectionResolver) : base(connectionResolver) { }
}
