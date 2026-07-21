using System;
using System.ComponentModel.DataAnnotations;
using Microsoft.Extensions.Options;

namespace One.Inception.Transport.RabbitMQ;

public interface IRabbitMqOptions
{
    int AdminPort { get; set; }
    int Port { get; set; }
    bool UseSsl { get; set; }
    string Password { get; set; }
    string Server { get; set; }
    string Username { get; set; }
    string VHost { get; set; }
    string ApiAddress { get; set; }
    public string BoundedContext { get; set; }
    FederatedExchangeOptions FederatedExchange { get; set; }
    IRabbitMqOptions GetOptionsFor(string boundedContext);

    public string ConnectionKey => DefaultConnectionKey(this);
    protected static string DefaultConnectionKey(IRabbitMqOptions c)
    {
        string connectionKey = $"{c.VHost}_{c.Server}".ToLower();
        return connectionKey;
    }
}
