using System.Diagnostics;
using System.Management.Automation;

namespace PSSharp.PowerShell.RabbitMQ.Commands;

[Cmdlet(VerbsLifecycle.Unregister, Noun)]
public sealed class RemoveRabbitMqManagementServer : Cmdlet
{
    private const string Noun = "RabbitMqManagementServer";

    [Parameter(Mandatory = true, ValueFromPipeline = true)]
    public RmqManagementServer? ManagementServer { get; set; }

    protected override void ProcessRecord()
    {
        Debug.Assert(ManagementServer is not null);

        if (!RmqManagementServer.Remove(ManagementServer.Uri))
        {
            var ex = new ItemNotFoundException(
                "The management server has already been unregistered."
            );
            var er = new ErrorRecord(
                ex,
                "NotRegistered",
                ErrorCategory.ObjectNotFound,
                ManagementServer
            );
            WriteError(er);
        }
    }
}
