namespace PSSharp.PowerShell.RabbitMQ.Commands.Management;

public sealed record PublishResponse
{
    public bool Routed { get; init; }
}
