using System.Text.Json.Serialization;

namespace PSSharp.PowerShell.RabbitMQ.Commands.Management;

public sealed record MessageRequest
{
    [JsonPropertyName("count")]
    public ulong Count { get; init; }

    [JsonPropertyName("ackmode")]
    public required string AckMode { get; init; }

    [JsonPropertyName("encoding")]
    public required string Encoding { get; init; }
}
