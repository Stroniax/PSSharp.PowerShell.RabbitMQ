using RabbitMQ.Client;

namespace PSSharp.PowerShell.RabbitMQ;

public sealed class PSRabbitSession(IConnection connection)
{
    private readonly IConnection _connection = connection;
    internal IChannel? DefaultChannel;

    public IConnection GetConnection()
    {
        return _connection;
    }
}
