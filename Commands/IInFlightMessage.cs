namespace PSSharp.PowerShell.RabbitMQ.Commands;

internal interface IInFlightMessage
{
    ulong DeliveryTag { get; }

    Task AckAsync(CancellationToken cancellationToken = default);

    Task NackAsync(bool requeue = false, CancellationToken cancellationToken = default);

    Task RejectAsync(bool requeue = false, CancellationToken cancellationToken = default);
}
