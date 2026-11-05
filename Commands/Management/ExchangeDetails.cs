using System.Text.Json.Serialization;

namespace PSSharp.PowerShell.RabbitMQ.Commands.Management;

public sealed record ExchangeDetails
{
    public required string Name { get; init; }
    public required string Type { get; init; }

    [JsonPropertyName("vhost")]
    public required string VHost { get; init; }
    public bool Durable { get; init; }
    public bool AutoDelete { get; init; }
    public bool Internal { get; init; }
    public required IDictionary<string, object?> Arguments { get; init; }
    public string? Policy { get; init; }
    public IDictionary<string, object?>? MessageStats { get; init; }
}
