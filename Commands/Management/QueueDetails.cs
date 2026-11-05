using System.Text.Json.Serialization;

namespace PSSharp.PowerShell.RabbitMQ.Commands.Management;

[JsonConverter(typeof(QueueDetailsJsonConverter))]
public sealed class QueueDetails
{
    public required string Name { get; init; }

    public required string VHost { get; init; }

    public uint Messages { get; init; }

    public DateTimeOffset? IdleSince { get; init; }

    public bool AutoDelete { get; init; }

    public int Consumers { get; init; }

    public IDictionary<string, object?>? AdditionalProperties { get; init; }

    public static implicit operator string(QueueDetails queue) => queue.Name;
}
