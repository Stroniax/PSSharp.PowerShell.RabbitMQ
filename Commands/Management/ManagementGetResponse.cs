using System.Text.Json.Serialization;
using RabbitMQ.Client;

namespace PSSharp.PowerShell.RabbitMQ.Commands.Management;

internal sealed record ManagementGetResponse
{
    [JsonPropertyName("payload_bytes")]
    public ulong PayloadBytes { get; init; }

    [JsonPropertyName("redelivered")]
    public bool Redelivered { get; init; }

    [JsonPropertyName("exchange")]
    public string Exchange { get; set; } = string.Empty;

    [JsonPropertyName("routing_key")]
    public string RoutingKey { get; set; } = string.Empty;

    [JsonPropertyName("message_count")]
    public uint MessageCount { get; set; }

    [JsonPropertyName("properties")]
    [JsonConverter(typeof(BasicPropertiesJsonConverter))]
    public IReadOnlyBasicProperties Properties { get; init; } = new BasicProperties();

    [JsonPropertyName("payload")]
    public string Payload { get; init; } = string.Empty;

    [JsonPropertyName("payload_encoding")]
    public string PayloadEncoding { get; init; } = string.Empty;
}
