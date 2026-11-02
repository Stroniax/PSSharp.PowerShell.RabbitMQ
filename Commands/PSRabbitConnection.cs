using System.Diagnostics.CodeAnalysis;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace PSSharp.PowerShell.RabbitMQ.Commands;

public sealed class PSRabbitConnection(IConnection connection)
{
    private readonly IConnection _connection = connection;
    private IChannel? _defaultChannel;

    internal async ValueTask<IChannel> GetDefaultChannel(CancellationToken cancellationToken)
    {
        return _defaultChannel ??= await _connection.CreateChannelAsync(
            new(true, true),
            cancellationToken
        );
    }

    internal bool TryGetDefaultChannel([MaybeNullWhen(false)] out IChannel channel)
    {
        channel = _defaultChannel;
        return channel is not null;
    }

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
