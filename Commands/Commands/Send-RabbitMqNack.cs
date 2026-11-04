using System.Diagnostics;
using System.Management.Automation;

namespace PSSharp.PowerShell.RabbitMQ.Commands;

[Cmdlet(
    VerbsCommunications.Send,
    Noun,
    SupportsShouldProcess = true,
    RemotingCapability = RemotingCapability.OwnedByCommand
)]
public sealed class SendRabbitMqNackCommand : RmqCmdlet
{
    private const string Noun = "RabbitMqNack";

    [Parameter(Mandatory = true, ValueFromPipeline = true, Position = 0)]
    [Alias("InputObject")]
    [ArgumentCompletions("$")]
    public PSRabbitMessage? Message { get; set; }

    [Parameter(ValueFromPipelineByPropertyName = true)]
    public SwitchParameter Requeue { get; set; }

    [Parameter]
    public SwitchParameter PassThru { get; set; }

    [Parameter]
    public SwitchParameter UseBasicReject { get; set; }

    protected override async Task ProcessRecordAsync()
    {
        Debug.Assert(Message is not null, "A value must be assigned to the mandatory parameter.");

        if (Message is not IInFlightMessage inFlight)
        {
            var ex = new ArgumentException(
                "The message is not owned by the broker. It cannot be negatively acknowledged. This may occur if the message was received from the RabbitMQ management API or with AutoAck=true.",
                nameof(Message)
            );
            var er = new ErrorRecord(ex, "NotInFlight", ErrorCategory.InvalidArgument, Message);
            await WriteErrorAsync(er);
            return;
        }

        if (inFlight.HasResponded())
        {
            var ex = new ArgumentException(
                "The message has already been responded to (whether by ack, nack, or otherwise).",
                nameof(Message)
            );
            var er = new ErrorRecord(
                ex,
                "AlreadyResponded",
                ErrorCategory.InvalidArgument,
                Message
            );
            await WriteErrorAsync(er);
            return;
        }

        if (!await ShouldProcessNack(inFlight.DeliveryTag).ConfigureAwait(false))
        {
            return;
        }

        try
        {
            if (UseBasicReject)
            {
                await inFlight.RejectAsync(Requeue, PipelineStopToken).ConfigureAwait(false);
            }
            else
            {
                await inFlight.NackAsync(Requeue, PipelineStopToken).ConfigureAwait(false);
            }
        }
        catch (Exception ex)
        {
            var er = new ErrorRecord(ex, "NackError", ErrorCategory.InvalidData, Message);
            await WriteErrorAsync(er).ConfigureAwait(false);
            return;
        }

        if (PassThru)
        {
            await WriteObjectAsync(Message);
        }
    }

    private Task<bool> ShouldProcessNack(ulong deliveryTag)
    {
        return InvokeAsync(() =>
            ShouldProcess(deliveryTag.ToString(), $"nack message with requeue={Requeue.IsPresent}")
        );
    }
}
