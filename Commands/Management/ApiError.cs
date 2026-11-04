using System.Text.Json.Serialization;

namespace PSSharp.PowerShell.RabbitMQ.Commands.Management;

public sealed record ApiError
{
    [JsonPropertyName("error")]
    public required string Error { get; init; }

    [JsonPropertyName("reason")]
    public string? Reason { get; init; }
}
