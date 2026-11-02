using System.Management.Automation;

namespace PSSharp.PowerShell.RabbitMQ.Commands;

[Cmdlet(VerbsLifecycle.Start, Noun)]
public sealed class StartRabbitMqConsumerCommand : RmqCmdlet
{
    public const string Noun = "RabbitMqConsumer";

    [Parameter(Mandatory = true, Position = 0, ValueFromPipelineByPropertyName = true)]
    [QueueNameArgumentCompleter]
    [Alias("QueueName")]
    public string Queue { get; set; } = string.Empty;

    /// <summary>
    /// Unregister the consumer after this number of messages have been processed. The consumer may continue
    /// running after this number of messages have been processed if there were messages in-flight.
    /// </summary>
    [Parameter]
    [ValidateRange(1ul, ulong.MaxValue)]
    [ArgumentCompletions("1", "100", "1000")]
    [Alias("MaxCount")]
    public ulong UnregisterAfterCount { get; set; } = ulong.MaxValue;

    /// <summary>
    /// Unregister the consumer after this interval has elapsed with no message to consume being delivered
    /// to the queue.
    /// </summary>
    [Parameter]
    [ArgumentCompletions("00:00:05", "00:00:30", "00:01:00")]
    [ValidateRange("00:00:01", "10675199.02:48:05.4775807")]
    public TimeSpan UnregisterAfterWindow { get; set; } = TimeSpan.MaxValue;

    /// <remarks>
    /// See <see cref="GetRabbitMqMessageCommand.ManualAck"/> for reasoning on inversion
    /// of RabbitMQ's counterpart argument.
    /// </remarks>
    [Parameter]
    public SwitchParameter ManualAck { get; init; }

    /// <summary>
    /// Sets the prefetch count on the channel that the consumer is registered with. Useful
    /// primarily in manual-ack mode to prevent draining the queue and being unable to process
    /// at the rate of delivery. Be aware, with auto-ack mode this is effectively ignored.
    /// It must be set if you are attempting to batch processing, because the default is 1. When
    /// the number of unacknowledged messages recevied by the consumer reaches this count, the
    /// broker will not deliver more messages to the consumer until another message is processed
    /// (via ack/nack).
    /// </summary>
    [Parameter]
    [ArgumentCompletions("1", "10", "100", "1000")]
    public ushort PrefetchCount { get; set; } = 1;

    [Parameter(Mandatory = true)]
    [RabbitConnectionCompleter]
    [RabbitConnection]
    public PSRabbitConnection Connection { get; set; } = null!;

    protected override async Task ProcessRecordAsync()
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
