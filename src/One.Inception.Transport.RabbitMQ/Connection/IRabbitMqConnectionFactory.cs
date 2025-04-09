using RabbitMQ.Client;

namespace One.Inception.Transport.RabbitMQ;

public interface IRabbitMqConnectionFactory
{
    IConnection CreateConnection();
    IConnection CreateConnectionWithOptions(IRabbitMqOptions options);
}
