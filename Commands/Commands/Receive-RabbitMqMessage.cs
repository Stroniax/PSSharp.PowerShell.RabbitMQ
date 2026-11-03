using System.Diagnostics;
using System.Management.Automation;

namespace PSSharp.PowerShell.RabbitMQ.Commands;

[Cmdlet(VerbsCommunications.Receive, Noun, DefaultParameterSetName = GetParameterSet)]
[OutputType(typeof(PSRabbitMessage), ParameterSetName = [GetParameterSet, PeekParameterSet])]
[OutputType(typeof(ConsumerJob), ParameterSetName = [ConsumeParameterSet])]
[Alias("rcrmq")]
public sealed class ReceiveRabbitMqMessageCommand : RmqCmdlet
{
    private const string Noun = "RabbitMqMessage";
    private const string PeekParameterSet = "Peek";
    private const string GetParameterSet = "Get";
    private const string ConsumeParameterSet = "Consume";

    [Parameter(Mandatory = true, Position = 0, ValueFromPipelineByPropertyName = true)]
    [QueueNameCompleter]
    [Alias("QueueName")]
    public string Queue { get; set; } = string.Empty;

    /// <summary>
    /// Raise an error if there is no message in the queue.
    /// </summary>
    [Parameter(ParameterSetName = PeekParameterSet)]
    [Parameter(ParameterSetName = GetParameterSet)]
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
    [Parameter(ParameterSetName = GetParameterSet)]
    [Parameter(ParameterSetName = ConsumeParameterSet)]
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

    /// <summary>
    /// Instead of receiving a single message, this parameter (and associated parameter set)
    /// creates a consumer job that continually receives messages as they are delivered according
    /// to the broker's implementation.
    /// </summary>
    [Parameter(Mandatory = true, ParameterSetName = ConsumeParameterSet)]
    [ValidateSet("true")]
    public SwitchParameter AsJob { get; set; }

    /// <summary>
    /// Unregister the consumer after this number of messages have been processed. The consumer may continue
    /// running after this number of messages have been processed if there were messages in-flight.
    /// </summary>
    [Parameter(ParameterSetName = ConsumeParameterSet)]
    [ValidateRange(1ul, ulong.MaxValue)]
    [ArgumentCompletions("1", "10", "100", "1000")]
    [Alias("MaxCount")]
    public ulong UnregisterAfterCount { get; set; } = ulong.MaxValue;

    /// <summary>
    /// Unregister the consumer after this interval has elapsed with no message to consume being delivered
    /// to the queue.
    /// </summary>
    [Parameter(ParameterSetName = ConsumeParameterSet)]
    [ArgumentCompletions("00:00:05", "00:00:30", "00:01:00")]
    [ValidateRange("00:00:01", "10675199.02:48:05.4775807")]
    public TimeSpan UnregisterAfterWindow { get; set; } = TimeSpan.MaxValue;

    /// <summary>
    /// Sets the prefetch count on the channel that the consumer is registered with. Useful
    /// primarily in manual-ack mode to prevent draining the queue and being unable to process
    /// at the rate of delivery. Be aware, with auto-ack mode this is effectively ignored.
    /// It must be set if you are attempting to batch processing, because the default is 1. When
    /// the number of unacknowledged messages recevied by the consumer reaches this count, the
    /// broker will not deliver more messages to the consumer until another message is processed
    /// (via ack/nack).
    /// </summary>
    [Parameter(ParameterSetName = ConsumeParameterSet)]
    [ArgumentCompletions("1", "10", "100", "1000")]
    public ushort PrefetchCount { get; set; } = 1;

    [Parameter(Mandatory = true)]
    [RabbitConnectionCompleter]
    [RabbitConnection]
    public PSRabbitConnection Connection { get; set; } = null!;

    protected override async Task ProcessRecordAsync()
    {
        switch (ParameterSetName)
        {
            case GetParameterSet:
            case PeekParameterSet:
                await Get();
                break;
            case ConsumeParameterSet:
                await Job();
                break;
            default:
                throw new UnreachableException("Is the parameter set implemented?");
        }
    }

    private async Task Get()
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

    private async Task Job()
    {
        // The job owns the channel, so create a new one for it.
        // This also prevents any error in the job from breaking other users of the DefaultChannel.
        var channel = await Connection
            .GetConnection()
            .CreateChannelAsync(null, PipelineStopToken)
            .ConfigureAwait(false);

        var job = new ConsumerJob(channel, Queue)
        {
            UnregisterAfterCount = UnregisterAfterCount,
            UnregisterAfterWindow = UnregisterAfterWindow,
            AutoAck = !ManualAck.IsPresent,
            PrefetchCount = PrefetchCount,
        };
        JobRepository.Add(job);
        await WriteObjectAsync(job).ConfigureAwait(false);
        _ = job.StartJobAsync();
    }
}
