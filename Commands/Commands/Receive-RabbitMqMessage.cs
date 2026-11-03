using System.Management.Automation;

namespace PSSharp.PowerShell.RabbitMQ.Commands;

[Cmdlet(VerbsCommunications.Receive, Noun, DefaultParameterSetName = AckParameterSet)]
[Alias("rcrmq")]
public sealed class ReceiveRabbitMqMessageCommand : RmqCmdlet
{
    private const string Noun = "RabbitMqMessage";
    private const string PeekParameterSet = "Peek";
    private const string AckParameterSet = "Ack";

    [Parameter(Mandatory = true, Position = 0, ValueFromPipelineByPropertyName = true)]
    [QueueNameCompleter]
    [Alias("QueueName")]
    public string Queue { get; set; } = string.Empty;

    /// <summary>
    /// Raise an error if there is no message in the queue.
    /// </summary>
    [Parameter]
    public SwitchParameter Mandatory { get; set; }

    /// <remarks>
    /// This is a flip from RabbitMQ's default because in PowerShell it sort of makes more sense
    /// to ack if the message is received, and require the user to do the more work of specifying
    /// "manual ack" in addition to the user manually acking the message. This makes the workflow
    /// easier to reason about for the average user:
    /// <code>
    /// Receive-RabbitMqMessage | Invoke-Handler # Ack'd, next Receive-* will not return the same message
    ///
    /// $Message = Receive-RabbitMqMessage -ManualAck
    /// try {
    ///     Invoke-ComplexHandler1 $Message -ErrorAction 'Stop'
    ///     Invoke-ComplexHandler2 $Message -ErrorAction 'Continue'
    ///     Send-RabbitMqAck $Message
    /// } catch {
    ///     Send-RabbitMqNack $Message -Return
    ///     throw
    /// }
    /// </code>
    /// </remarks>
    [Parameter(ParameterSetName = AckParameterSet)]
    public SwitchParameter ManualAck { get; set; }

    /// <summary>
    /// Return the message to the queue. This is similar to using the RabbitMQ management API to "peek" or
    /// see a message "without" consuming it from the queue. In reality, this is not a safe/idempotent
    /// operation. Peeking at a message will cause its "Redelivered" status to be set next time it is
    /// delivered. The "Peek" implementation  will be immediately nack the message with requeue=true.
    /// </summary>
    [Parameter(ParameterSetName = PeekParameterSet)]
    public SwitchParameter Peek { get; set; }

    /// <summary>
    /// How to deserialize the message if it should be deserialized regardless of the content type.
    /// </summary>
    [Parameter]
    public PSRabbitMessageSerialization? Serialization { get; set; }

    [Parameter(Mandatory = true)]
    [RabbitConnectionCompleter]
    [RabbitConnection]
    public PSRabbitConnection Connection { get; set; } = null!;

    protected override async Task ProcessRecordAsync()
    {
        var channel = await Connection.GetDefaultChannel(PipelineStopToken).ConfigureAwait(false);

        var autoAck = !ManualAck.IsPresent && !Peek.IsPresent;
        var message = await channel.BasicGetAsync(Queue, autoAck, PipelineStopToken);

        // Return the message to the queue immediately, before any further handling.
        if (message is not null && Peek.IsPresent)
        {
            // Do not permit cancellation of requeue for "Peek"
            await channel.BasicNackAsync(message.DeliveryTag, false, true, CancellationToken.None);
        }

        if (message is null && Mandatory.IsPresent)
        {
            var ex = new ItemNotFoundException("There are no messages in the queue.");
            var er = new ErrorRecord(ex, "QueueEmpty", ErrorCategory.InvalidData, Queue);
            await WriteErrorAsync(er);
        }

        if (message is null)
        {
            return;
        }

        if (
            PSRabbitMessage.FromGet(
                channel,
                message,
                Serialization,
                out var psmessage,
                out var error
            )
        )
        {
            await WriteObjectAsync(psmessage);
        }
        else
        {
            await WriteErrorAsync(error);
        }
    }
}
