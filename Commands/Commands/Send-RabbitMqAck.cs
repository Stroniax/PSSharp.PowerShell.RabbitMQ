using System.Diagnostics;
using System.Management.Automation;

namespace PSSharp.PowerShell.RabbitMQ.Commands;

[Cmdlet(
    VerbsCommunications.Send,
    Noun,
    SupportsShouldProcess = true,
    RemotingCapability = RemotingCapability.OwnedByCommand
)]
public sealed class SendRabbitMqAckCommand : RmqCmdlet
{
    private const string Noun = "RabbitMqAck";

    [Parameter(Mandatory = true, ValueFromPipeline = true)]
    [Alias("InputObject")]
    [ArgumentCompletions("$")]
    public PSRabbitMessage? Message { get; set; }

    protected override async Task ProcessRecordAsync()
    {
        Debug.Assert(Message is not null, "Message must be assigned to Mandatory parameter.");

        if (!await InvokeAsync(() => ShouldProcess(Message.DeliveryTag.ToString(), "ack message")))
        {
            return;
        }

        try
        {
            await Message.AckAsync(PipelineStopToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            var er = new ErrorRecord(ex, "AckError", ErrorCategory.InvalidData, Message);
            await WriteErrorAsync(er).ConfigureAwait(false);
        }
    }
}
