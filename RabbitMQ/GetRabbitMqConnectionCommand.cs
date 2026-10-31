using System.Management.Automation;

namespace PSSharp.PowerShell.RabbitMQ;

[Cmdlet(VerbsCommon.Get, Noun)]
public sealed class GetRabbitMqConnectionCommand : PSCmdlet
{
    private const string Noun = "RabbitMqConnection";

    public const string InstanceParameterSet = "Instance";

    [Parameter(Mandatory = false, ValueFromPipelineByPropertyName = true, ParameterSetName = InstanceParameterSet)]
    public Guid[] InstanceId { get; set; } = [];

    protected override void ProcessRecord()
    {
        var store = new RabbitConnectionStore(SessionState.PSVariable);

        if (ParameterSetName == InstanceParameterSet && InstanceId.Length == 0)
        {
            WriteObject(store.GetConnections(), true);
            return;
        }

        var hs = new HashSet<Guid>(InstanceId);

        foreach (var cx in store.GetConnections())
        {
            if (hs.Remove(cx.InstanceId))
            {
                WriteObject(cx);
            }
        }

        foreach (var missing in hs)
        {
            var exn = new ItemNotFoundException("RabbitMQ connection with the specified InstanceId was not found.");
            var err = new ErrorRecord(exn, "RabbitMqConnectionNotFound", ErrorCategory.ObjectNotFound, missing);
            WriteError(err);
        }
    }
}
