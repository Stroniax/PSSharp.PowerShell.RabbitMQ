using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Diagnostics.CodeAnalysis;

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
    /// A user-facing name of the connection.
    /// </summary>
    public required string Name { get; init; }

    /// <summary>
    /// Client provided name.
    /// </summary>
    public string? ClientProvidedName => _connection.ClientProvidedName;

    /// <summary>
    /// Whether the connection is still open versus having been closed.
    /// </summary>
    [MemberNotNullWhen(true, nameof(CloseReason))]
    public bool IsOpen => _connection.IsOpen;

    public ShutdownEventArgs? CloseReason => _connection.CloseReason;

    public IDictionary<string, object?>? ServerProperties => _connection.ServerProperties;

    /// <summary>
    /// Gets the inner connection managed by this session.
    /// </summary>
    /// <returns></returns>
    public IConnection GetConnection()
    {
        return _connection;
    }
}
