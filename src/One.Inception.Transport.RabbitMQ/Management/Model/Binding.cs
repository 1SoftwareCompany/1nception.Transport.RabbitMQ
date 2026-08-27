using System.Collections.Generic;
using System.Text.Json.Serialization;

namespace One.Inception.Transport.RabbitMQ.Management.Model;

public sealed record Binding
{
    [JsonPropertyName("source")]
    public string Source { get; set; }

    [JsonPropertyName("vhost")]
    public string Vhost { get; set; }

    [JsonPropertyName("destination")]
    public string Destination { get; set; }

    [JsonPropertyName("destination_type")]
    public string DestinationType { get; set; }

    [JsonPropertyName("routing_key")]
    public string RoutingKey { get; set; }

    [JsonPropertyName("arguments")]
    public Dictionary<string, object?> Arguments { get; set; } = new();

    [JsonPropertyName("properties_key")]
    public string PropertiesKey { get; set; }
}
