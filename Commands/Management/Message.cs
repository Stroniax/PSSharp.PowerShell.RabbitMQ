using RabbitMQ.Client;

namespace PSSharp.PowerShell.RabbitMQ.Commands.Management;

public sealed record Message
{
    public ulong PayloadBytes { get; init; }

    public bool Redelivered { get; init; }

    public string Exchange { get; set; } = string.Empty;

    public string RoutingKey { get; set; } = string.Empty;

    public uint MessageCount { get; set; }

    public IReadOnlyBasicProperties Properties { get; init; } = new BasicProperties();

    public string Payload { get; init; } = string.Empty;

    public string PayloadEncoding { get; init; } = string.Empty;
}
