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
    string BoundedContext { get; set; }
    FederatedExchangeOptions FederatedExchange { get; set; }
    IRabbitMqOptions GetOptionsFor(string boundedContext);

    /// <summary>
    /// Max number of channels that may be created dynamically for publish.
    /// </summary>
    int MaxChannelsForPublish { get; set; }

    /// <summary>
    /// Seconds to wait to acquire a lease. Throws exception if this time is exceeded. Deadlock protection
    /// </summary>
    int TimeoutForChannelLease { get; set; }


    public string ConnectionKey { get; }

}
