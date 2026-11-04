using System.Text.Json.Serialization;

namespace PSSharp.PowerShell.RabbitMQ.Commands.Management;

public sealed record QueueDeclareOptions
{
    [JsonPropertyName("auto_delete")]
    public bool AutoDelete { get; init; }

    [JsonPropertyName("durable")]
    public bool Durable { get; init; }

    [JsonPropertyName("arguments")]
    public IDictionary<string, object?> Arguments { get; init; } =
        new Dictionary<string, object?>();

    [JsonPropertyName("node")]
    public string? Node { get; init; }
}
