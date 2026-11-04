using System.Diagnostics;
using System.Management.Automation;
using PSSharp.PowerShell.RabbitMQ.Commands.Management;

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
    private const string ManagementParameterSet = "Management";

    [Parameter(Mandatory = true, Position = 0, ValueFromPipelineByPropertyName = true)]
    [QueueNameCompleter]
    [Alias("QueueName")]
    public string Queue { get; set; } = string.Empty;

    /// <summary>
    /// Raise an error if no message can be delivered. If there are any messages, even if the quantity
    /// does not reach a specified <c>-Count</c>, no error will be emitted.
    /// </summary>
    [Parameter(ParameterSetName = PeekParameterSet)]
    [Parameter(ParameterSetName = GetParameterSet)]
    [Parameter(ParameterSetName = ManagementParameterSet)]
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
    /// <para>
    /// When consuming with a dedicated consumer (via <c>-AsJob</c>), the consumer will unregister after this
    /// number of messages has been processed. The consumer may have received more messages according to the
    /// <see cref="PrefetchCount"/> which it will continue to process even after sending the <c>basic.cancel</c>.
    /// </para>
    /// <para>
    /// For <c>basic.get</c> (except <c>-Peek</c> which does not support <c>-Count</c> due to possible sequencing
    /// behavior and duplicate deliveries that may arise) or the management API (via <c>-ManagementUri</c>),
    /// the cmdlet will attempt to retrieve up to this number of messages. If there are not this many messages
    /// available, the cmdlet will only retrieve until the endpoint does not deliver a message. See note on the
    /// <c>-Mandatory</c> parameter for related behavior.
    /// </para>
    /// </summary>
    [Parameter(ParameterSetName = ConsumeParameterSet)]
    [Parameter(ParameterSetName = ManagementParameterSet)]
    [Parameter(ParameterSetName = GetParameterSet)]
    [ValidateRange(1ul, ulong.MaxValue)]
    [ArgumentCompletions("1", "10", "100", "1000")]
    public ulong Count { get; set; } = 1;

    /// <summary>
    /// Unregister the consumer after this interval has elapsed with no message to consume being delivered
    /// to the queue.
    /// </summary>
    [Parameter(ParameterSetName = ConsumeParameterSet)]
    [ArgumentCompletions("00:00:05", "00:00:30", "00:01:00")]
    [ValidateRange("00:00:01", "49.17:02:47.294")]
    public TimeSpan UnregisterAfterWindow { get; set; } =
        TimeSpan.FromMilliseconds(uint.MaxValue - 1);

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

    [Parameter(Mandatory = true, ParameterSetName = ConsumeParameterSet)]
    [Parameter(Mandatory = true, ParameterSetName = GetParameterSet)]
    [Parameter(Mandatory = true, ParameterSetName = PeekParameterSet)]
    [RabbitConnectionCompleter]
    [RabbitConnection]
    public PSRabbitConnection? Connection { get; set; }

    [Parameter(Mandatory = true, ParameterSetName = ManagementParameterSet)]
    [ManagementServerUriCompleter]
    [ValidateAbsoluteUri]
    [ValidateUriScheme("http", "https")]
    public Uri? ManagementUri { get; set; }

    [Parameter(ParameterSetName = ManagementParameterSet)]
    [Credential]
    [ArgumentCompletions("guest")]
    public PSCredential? Credential { get; set; }

    [Parameter(ParameterSetName = ManagementParameterSet)]
    [ValidateNotNullOrEmpty]
    [ArgumentCompletions("/")]
    public string VHost { get; set; } = "/";

    /// <summary>
    ///
    /// </summary>
    /// <remarks>
    /// It defaults to ack without requeue because that is the behavior a user would expect if they were
    /// receiving a message from the broker. This exhibits consistent behavior until specific parameters
    /// are set to indicate otherwise - whether <c>-Peek</c>, <c>-ManualAck</c>, or <c>-AckMode
    /// ack_requeue_true</c>.
    /// </remarks>
    [Parameter(ParameterSetName = ManagementParameterSet)]
    [ValidateNotNullOrEmpty]
    [ValidateSet(
        "ack_requeue_true",
        "reject_requeue_true",
        "ack_requeue_false",
        "reject_requeue_false"
    )]
    public string AckMode
    {
        get;
        set => field = value.ToLower();
    } = "ack_requeue_false";

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
            case ManagementParameterSet:
                await Management();
                break;
            default:
                throw new UnreachableException("Is the parameter set implemented?");
        }
    }

    private async Task Get()
    {
        Debug.Assert(
            Connection is not null,
            "Mandatory parameter 'Connection' for 'Get' or 'Peek' parameter set cannot be null."
        );

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
            var er = ErrorMandatoryMessage();
            await WriteErrorAsync(er);
        }

        if (message is null)
        {
            return;
        }

        if (
            !PSRabbitMessage.FromGet(
                ManualAck.IsPresent ? channel : null,
                message,
                Serialization,
                out var psmessage,
                out var error
            )
        )
        {
            await WriteErrorAsync(error);
        }
        await WriteObjectAsync(psmessage);

        // Retrieve up to `-Count $SomeNumber` messages or until the queue is empty
        // For -Peek, the Count must be 1 (default value, not allowed to specify in parameter set)
        for (ulong i = 1; i < Count; i++)
        {
            message = await channel.BasicGetAsync(Queue, autoAck, PipelineStopToken);

            if (message is null)
            {
                break;
            }

            if (
                !PSRabbitMessage.FromGet(
                    ManualAck.IsPresent ? channel : null,
                    message,
                    Serialization,
                    out psmessage,
                    out error
                )
            )
            {
                await WriteErrorAsync(error);
            }
            await WriteObjectAsync(psmessage);
        }
    }

    private ErrorRecord ErrorMandatoryMessage()
    {
        var ex = new ItemNotFoundException("There are no messages in the queue.");
        var er = new ErrorRecord(ex, "QueueEmpty", ErrorCategory.InvalidData, Queue);
        return er;
    }

    private async Task Job()
    {
        Debug.Assert(
            Connection is not null,
            "Mandatory parameter 'Connection' for 'Consume' parameter set cannot be null."
        );
        // The job owns the channel, so create a new one for it.
        // This also prevents any error in the job from breaking other users of the DefaultChannel.
        var channel = await Connection
            .GetConnection()
            .CreateChannelAsync(null, PipelineStopToken)
            .ConfigureAwait(false);

        var job = new ConsumerJob(channel, Queue)
        {
            UnregisterAfterCount = MyInvocation.BoundParameters.ContainsKey(nameof(Count))
                ? Count
                : ulong.MaxValue,
            UnregisterAfterWindow = UnregisterAfterWindow,
            AutoAck = !ManualAck.IsPresent,
            PrefetchCount = PrefetchCount,
            Serialization = Serialization,
        };
        JobRepository.Add(job);
        await WriteObjectAsync(job).ConfigureAwait(false);
        _ = job.StartJobAsync();
    }

    private async Task Management()
    {
        Debug.Assert(
            ManagementUri is not null,
            "Mandatory parameter 'ManagementUri' for 'Management' parameter set cannot be null."
        );

        var cred = Credential?.GetNetworkCredential() ?? ManagementUri.GetNetworkCredential();
        var uri = ManagementUri.WithoutCredential();

        if (cred is null)
        {
            await this.ManagementCredentialRequiredError(ManagementUri);
            return;
        }

        using var client = new ManagementClient(uri, cred);
        var messages = await client.GetMessages(
            VHost,
            Queue,
            Count,
            AckMode,
            "base64",
            PipelineStopToken
        );

        foreach (var message in messages)
        {
            if (
                !PSRabbitMessage.FromManagement(
                    message,
                    Serialization,
                    out var psmessage,
                    out var pserror
                )
            )
            {
                await WriteErrorAsync(pserror);
            }
            await WriteObjectAsync(psmessage);
        }

        // Batch pipeline callback to write errors into a single callback
        if (Mandatory.IsPresent && messages.Length == 0)
        {
            var er = ErrorMandatoryMessage();
            await WriteErrorAsync(er);
        }
    }
}
