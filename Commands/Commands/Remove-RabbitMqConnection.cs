using System.Management.Automation;

namespace PSSharp.PowerShell.RabbitMQ.Commands;

[Cmdlet(
    VerbsCommon.Remove,
    Noun,
    DefaultParameterSetName = SessionParameterSet,
    SupportsShouldProcess = true
)]
public sealed class RemoveRabbitMqConnectionCommand : RmqCmdlet
{
    private const string Noun = "RabbitMqConnection";

    public const string NameParameterSet = "Name";
    public const string SessionParameterSet = "Session";
    public const string InstanceParameterSet = "InstanceId";

    [Parameter(
        ParameterSetName = SessionParameterSet,
        Mandatory = true,
        Position = 0,
        ValueFromPipeline = true
    )]
    [RabbitConnection]
    [RabbitConnectionCompleter]
    public PSRabbitConnection? Connection { get; set; }

    [Parameter(
        ParameterSetName = InstanceParameterSet,
        Mandatory = true,
        Position = 0,
        ValueFromPipelineByPropertyName = true
    )]
    [RabbitConnectionCompleter]
    public Guid InstanceId { get; init; }

    private readonly RabbitConnectionStore _store = RabbitConnectionStore.Current;

    protected override async Task ProcessRecordAsync()
    {
        if (Connection is not null)
        {
            if (!_store.Remove(Connection))
            {
                var ex = new InvalidOperationException(
                    "The RabbitMQ connection has already been removed."
                );
                var er = new ErrorRecord(
                    ex,
                    "AlreadyRemoved",
                    ErrorCategory.InvalidArgument,
                    Connection
                );
                await WriteErrorAsync(er).ConfigureAwait(false);
            }

            return;
        }

        var found = false;
        foreach (var connection in _store.GetConnections())
        {
            if (connection.InstanceId == InstanceId || connection == Connection)
            {
                if (ShouldProcess(connection.InstanceId.ToString(), "Remove RabbitMQ Connection"))
                {
                    await using var _context = connection.GetConnection().ConfigureAwait(false);
                    _store.Remove(connection);
                    found = true;
                    break;
                }
            }
        }

        if (!found)
        {
            var ex = new ItemNotFoundException(
                "A RabbitMQ connection with the specified InstanceId does not exist."
            );
            var er = new ErrorRecord(
                ex,
                "InstanceIdNotFound",
                ErrorCategory.ObjectNotFound,
                InstanceId
            )
            {
                ErrorDetails = new(
                    $"A RabbitMQ connection with the specified InstanceId \"{InstanceId}\" does not exist."
                ),
            };
            await WriteErrorAsync(er).ConfigureAwait(false);
        }
    }
}
