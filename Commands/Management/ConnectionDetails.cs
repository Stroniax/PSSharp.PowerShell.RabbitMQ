using System.Text.Json.Serialization;

namespace PSSharp.PowerShell.RabbitMQ.Commands.Management;

public record UserConnectionDetails
{
    public required string Name { get; init; }
    public required string Node { get; init; }
    public required string User { get; init; }

    [JsonPropertyName("vhost")]
    public required string VHost { get; init; }
}

public sealed record ConnectionDetails : UserConnectionDetails
{
    public required string AuthMechanism { get; init; }
    public ulong ConnectedAt { get; init; }
    public IDictionary<string, object?>? GarbageCollection { get; init; }
    public required IDictionary<string, object?> ClientProperties { get; init; }
    public required string Host { get; init; }

    public required string PeerHost { get; init; }
    public required ushort PeerPort { get; init; }
    public required string Protocol { get; init; }

    public required bool Ssl { get; set; }
    public string? SslCipher { get; init; }
    public string? SslHash { get; init; }
    public string? SslProtocol { get; init; }

    public required string State { get; init; }
    public uint Timeout { get; init; }
    public required string Type { get; init; }
    public required string UserProvidedName { get; init; }
    public required string UserWhoPerformedAction { get; init; }
}
