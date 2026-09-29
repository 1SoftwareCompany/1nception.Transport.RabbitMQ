using RabbitMQ.Client;
using System.Threading.Tasks;

namespace One.Inception.Transport.RabbitMQ;

public interface IRabbitMqConnectionFactory
{
    Task<IConnection> CreateConnectionAsync(string connectionName = null);
    Task<IConnection> CreateConnectionWithOptionsAsync(IRabbitMqOptions options, string connectionName = null);
}
