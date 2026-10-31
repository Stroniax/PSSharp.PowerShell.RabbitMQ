using RabbitMQ.Client;

namespace PSSharp.PowerShell.RabbitMQ;

public sealed class PSRabbitConnection(IConnection connection)
{
    private readonly IConnection _connection = connection;
    internal IChannel? DefaultChannel;

    /// <summary>
    /// An identifier unique to the session/connection.
    /// </summary>
    public Guid InstanceId { get; } = Guid.NewGuid();

    /// <summary>
    /// Gets the inner connection managed by this session.
    /// </summary>
    /// <returns></returns>
    public IConnection GetConnection()
    {
        return _connection;
    }
}
