using System.Text.Json.Serialization;

namespace PSSharp.PowerShell.RabbitMQ.Commands.Management;

[JsonConverter(typeof(JsonStringEnumConverter<DestinationType>))]
public enum DestinationType
{
    [JsonPropertyName("queue")]
    Queue = 1,

    [JsonPropertyName("exchange")]
    Exchange = 2,
}
