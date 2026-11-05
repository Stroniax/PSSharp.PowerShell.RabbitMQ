using System.Management.Automation;

namespace PSSharp.PowerShell.RabbitMQ.Commands;

[Cmdlet(VerbsCommon.Get, Nouns.RabbitMqManagementServer)]
public sealed class GetRabbitMqManagementServer : Cmdlet
{
    protected override void ProcessRecord()
    {
        WriteObject(RmqManagementServer.All(), true);
    }
}
