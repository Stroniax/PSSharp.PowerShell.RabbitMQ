using System.Text.Json.Serialization;

namespace PSSharp.PowerShell.RabbitMQ.Commands.Management;

public sealed record ManagementQueueResponse
{
    [JsonPropertyName("name")]
    public required string Name { get; init; }

    [JsonPropertyName("messages")]
    public uint Messages { get; init; }

    [JsonPropertyName("idle_since")]
    public DateTimeOffset? IdleSince { get; init; }

    [JsonPropertyName("auto_delete")]
    public bool AutoDelete { get; init; }

    [JsonPropertyName("consumers")]
    public int Consumers { get; init; }

    [JsonExtensionData]
    public IDictionary<string, object?>? AdditionalProperties { get; set; }

    public static implicit operator string(ManagementQueueResponse queue) => queue.Name;
}
