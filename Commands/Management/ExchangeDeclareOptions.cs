using System.Collections.Immutable;

namespace PSSharp.PowerShell.RabbitMQ.Commands.Management;

public sealed record ExchangeDeclareOptions
{
    public required string Type { get; init; }
    public bool AutoDelete { get; init; }
    public bool Durable { get; init; }
    public bool Internal { get; init; }
    public ImmutableDictionary<string, object?> Arguments { get; init; } = [];
}
