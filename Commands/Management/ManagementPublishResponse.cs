namespace PSSharp.PowerShell.RabbitMQ.Commands.Management;

internal sealed record ManagementPublishResponse
{
    public bool Routed { get; init; }
}
