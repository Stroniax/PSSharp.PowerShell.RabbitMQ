namespace PSSharp.PowerShell.RabbitMQ.Commands.Management;

internal sealed record DeleteQueueOptions
{
    public bool IfEmpty { get;init; }
    public bool IfUnused { get; init; }
}