namespace PSSharp.PowerShell.RabbitMQ.Commands.Management;

public sealed record CreateBinding
{
    public required IDictionary<string, object?> Arguments { get; init; }
    public required string RoutingKey { get; init; }
}
