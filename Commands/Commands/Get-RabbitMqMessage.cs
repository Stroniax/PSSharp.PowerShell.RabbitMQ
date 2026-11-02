using System.Management.Automation;

namespace PSSharp.PowerShell.RabbitMQ.Commands;

[Cmdlet(VerbsCommon.Get, Noun)]
public sealed class GetRabbitMqMessageCommand : RmqCmdlet
{
    private const string Noun = "RabbitMqMessage";

    [Parameter(Mandatory = true, Position = 0, ValueFromPipelineByPropertyName = true)]
    [QueueNameArgumentCompleter]
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
    /// Get-RabbitMqMessage | Invoke-Handler # Ack'd, next Get-* will not return the same message
    ///
    /// $Message = Get-RabbitMqMessage -ManualAck
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
    [Parameter]
    public SwitchParameter ManualAck { get; set; }

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

        var autoAck = !ManualAck.IsPresent;
        var message = await channel.BasicGetAsync(Queue, autoAck, PipelineStopToken);

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
