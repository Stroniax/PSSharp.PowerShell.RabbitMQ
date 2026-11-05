namespace PSSharp.PowerShell.RabbitMQ.Commands.Management;

public sealed record VirtualHost
{
    public required string Name { get; init; }
    public ulong Messages { get; init; }
    public string? Description { get; init; }
    public required IDictionary<string, object?> Metadata { get; init; }
    public required string[] Tags { get; init; }
    public string? DefaultQueueType { get; init; }
    public ulong? MessagesReady { get; init; }
    public ulong? MessagesUnacknowledged { get; init; }
    public bool ProtectedFromDeletion { get; init; }
    public bool Tracing { get; init; }
    public IDictionary<string, object?>? MessageStats { get; init; }
}
