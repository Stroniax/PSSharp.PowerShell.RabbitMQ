using RabbitMQ.Client;
using System.Text.Json.Serialization;

namespace PSSharp.PowerShell.RabbitMQ.Commands.Management;

public sealed record PublishRequest
{

    [JsonPropertyName("properties")]
    [JsonConverter(typeof(BasicPropertiesJsonConverter))]
    public IReadOnlyBasicProperties? Properties { get; init; }

    [JsonPropertyName("routing_key")]
    public required string RoutingKey { get; init; }

    [JsonPropertyName("payload")]
    public required string Payload { get; init; }

    [JsonPropertyName("payload_encoding")]
    public required string PayloadEncoding { get; init; }

}