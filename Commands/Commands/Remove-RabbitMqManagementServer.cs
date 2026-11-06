using System.Diagnostics;
using System.Management.Automation;

namespace PSSharp.PowerShell.RabbitMQ.Commands;

[Cmdlet(VerbsLifecycle.Unregister, Nouns.RabbitMqManagementServer)]
public sealed class RemoveRabbitMqManagementServer : Cmdlet
{
    [Parameter(Mandatory = true, ValueFromPipeline = true)]
    public RmqManagementServer? ManagementServer { get; set; }

    protected override void ProcessRecord()
    {
        Assert.MandatoryParameter(ManagementServer, "all");

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
