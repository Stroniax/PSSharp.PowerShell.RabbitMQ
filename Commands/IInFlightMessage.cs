namespace PSSharp.PowerShell.RabbitMQ.Commands;

internal interface IInFlightMessage
{
    /// <summary>
    /// Delivery tag which is just metadata; <see cref="AckAsync"/> and similar handle the response
    /// in a self-contained manner.
    /// </summary>
    ulong DeliveryTag { get; }

    Task AckAsync(CancellationToken cancellationToken = default);

    Task NackAsync(bool requeue = false, CancellationToken cancellationToken = default);

    Task RejectAsync(bool requeue = false, CancellationToken cancellationToken = default);

    /// <summary>
    /// Checks whether the current instance has been responded to. A response must not be dispatched multiple
    /// times.
    /// </summary>
    bool HasResponded();
}
