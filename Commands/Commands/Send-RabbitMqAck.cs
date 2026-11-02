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

    [Parameter(Mandatory = true, ValueFromPipeline = true, Position = 0)]
    [Alias("InputObject")]
    [ArgumentCompletions("$")]
    public PSRabbitMessage? Message { get; set; }

    [Parameter]
    public SwitchParameter PassThru { get; set; }

    protected override async Task ProcessRecordAsync()
    {
        Debug.Assert(Message is not null, "A value must be assigned to the mandatory parameter.");

        if (!await ShouldProcessAck(Message.DeliveryTag).ConfigureAwait(false))
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
            return;
        }

        if (PassThru)
        {
            await WriteObjectAsync(Message);
        }
    }

    private Task<bool> ShouldProcessAck(ulong deliveryTag)
    {
        return InvokeAsync(() => ShouldProcess(deliveryTag.ToString(), "ack message"));
    }
}
