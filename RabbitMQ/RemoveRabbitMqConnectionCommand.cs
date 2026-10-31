using System.Management.Automation;

namespace PSSharp.PowerShell.RabbitMQ;

[Cmdlet(VerbsCommon.Remove, Noun, DefaultParameterSetName = InstanceParameterSet, SupportsShouldProcess = true)]
public sealed class RemoveRabbitMqConnectionCommand : RmqCmdlet
{
    private const string Noun = "RabbitMqConnection";

    public const string SessionParameterSet = "Session";
    public const string InstanceParameterSet = "InstanceId";


    [Parameter(ParameterSetName = SessionParameterSet, Mandatory = true, ValueFromPipeline = true)]
    public PSRabbitConnection? Connection { get; set; }

    [Parameter(ParameterSetName = InstanceParameterSet, Mandatory = true, Position = 0)]
    public Guid InstanceId { get; init; }

    protected override async Task ProcessRecordAsync()
    {
        var store = new RabbitConnectionStore(SessionState.PSVariable);
        foreach (var connection in store.GetConnections())
        {
            if (connection.InstanceId == InstanceId || connection == Connection)
            {
                if (ShouldProcess(connection.InstanceId.ToString(), "Remove RabbitMQ Connection"))
                {
                    store.Remove(connection);
                    await connection.GetConnection().DisposeAsync();
                }
            }
        }
    }
}
