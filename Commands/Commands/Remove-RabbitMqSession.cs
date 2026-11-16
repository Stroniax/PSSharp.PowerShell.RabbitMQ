using System.Management.Automation;

namespace PSSharp.PowerShell.RabbitMQ.Commands;

[Cmdlet(
    VerbsCommon.Remove,
    Nouns.RabbitMqSession,
    DefaultParameterSetName = SessionParameterSet,
    SupportsShouldProcess = true
)]
public sealed class RemoveRabbitMqSessionCommand : RmqCmdlet
{
    public const string NameParameterSet = "Name";
    public const string SessionParameterSet = "Session";
    public const string InstanceParameterSet = "InstanceId";

    [Parameter(
        ParameterSetName = SessionParameterSet,
        Mandatory = true,
        Position = 0,
        ValueFromPipeline = true
    )]
    [Alias("Connection")]
    [PSRabbitSession]
    [PSRabbitSessionCompleter]
    public PSRabbitSession? Session { get; set; }

    [Parameter(
        ParameterSetName = InstanceParameterSet,
        Mandatory = true,
        Position = 0,
        ValueFromPipelineByPropertyName = true
    )]
    [PSRabbitSessionCompleter]
    public Guid InstanceId { get; init; }

    private readonly PSRabbitSessionStore _store = PSRabbitSessionStore.Current;

    protected override async Task ProcessRecordAsync()
    {
        if (Session is not null)
        {
            if (!_store.Remove(Session))
            {
                var ex = new InvalidOperationException(
                    "The RabbitMQ session has already been removed."
                );
                var er = new ErrorRecord(
                    ex,
                    "AlreadyRemoved",
                    ErrorCategory.InvalidArgument,
                    Session
                );
                await WriteErrorAsync(er).ConfigureAwait(false);
            }

            await Session.DisposeAsync();
            return;
        }

        var found = false;
        foreach (var session in _store.GetSessions())
        {
            if (session.InstanceId == InstanceId || session == Session)
            {
                if (ShouldProcess(session.InstanceId.ToString(), "Remove RabbitMQ session"))
                {
                    await using var _context = session;
                    _store.Remove(session);
                    found = true;
                    break;
                }
            }
        }

        if (!found)
        {
            var er = Assert.ItemNotFound("RabbitMQ session", InstanceId);
            await WriteErrorAsync(er).ConfigureAwait(false);
        }
    }
}
