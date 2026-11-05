namespace PSSharp.PowerShell.RabbitMQ.Commands.Management;

public sealed record DeleteExchangeOptions
{
    public bool IfUnused { get; init; }
}