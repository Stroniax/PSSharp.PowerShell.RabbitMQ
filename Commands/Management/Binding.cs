using System.Text.Json.Serialization;

namespace PSSharp.PowerShell.RabbitMQ.Commands.Management;

public sealed record Binding
{
    [JsonPropertyName("vhost")]
    public required string VHost { get; init; }
    public required string Source { get; init; }
    public required string Destination { get; init; }
    public required DestinationType DestinationType { get; init; }
    public required string RoutingKey { get; init; }
    public required IDictionary<string, object?> Arguments { get; init; }
    public required string PropertiesKey { get; init; }
}
