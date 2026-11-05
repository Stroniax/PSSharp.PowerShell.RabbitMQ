using System.Management.Automation;

namespace PSSharp.PowerShell.RabbitMQ.Commands;

[Cmdlet(VerbsCommon.Get, Noun)]
public sealed class GetRabbitMqManagementServer : Cmdlet
{
    private const string Noun = "RabbitMqManagementServer";

    protected override void ProcessRecord()
    {
        WriteObject(RmqManagementServer.All(), true);
    }
}
