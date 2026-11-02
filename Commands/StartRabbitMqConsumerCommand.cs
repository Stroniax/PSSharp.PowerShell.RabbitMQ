using System.Management.Automation;

namespace PSSharp.PowerShell.RabbitMQ.Commands;

[Cmdlet(VerbsLifecycle.Start, Noun)]
public sealed class StartRabbitMqConsumerCommand : RmqCmdlet
{
    public const string Noun = "RabbitMqConsumer";

    [Parameter(Mandatory = true, Position = 0, ValueFromPipelineByPropertyName = true)]
    [Alias("QueueName")]
    public string Queue { get; set; } = string.Empty;

    /// <summary>
    /// Unregister the consumer after this number of messages have been processed. The consumer may continue
    /// running after this number of messages have been processed if there were messages in-flight.
    /// </summary>
    [Parameter]
    [ValidateRange(1ul, ulong.MaxValue)]
    [Alias("MaxCount")]
    public ulong UnregisterAfterCount { get; set; } = ulong.MaxValue;

    /// <summary>
    /// Unregister the consumer after this interval has elapsed with no message to consume being delivered
    /// to the queue.
    /// </summary>
    [Parameter]
    [ValidateRange("00:00:01", "10675199.02:48:05.4775807")]
    public TimeSpan UnregisterAfterWindow { get; set; } = TimeSpan.MaxValue;

    [Parameter(Mandatory = true)]
    [RabbitConnectionCompleter]
    [RabbitConnection]
    public PSRabbitConnection Connection { get; set; } = null!;

    protected override async Task ProcessRecordAsync()
    {
        var channel = await Connection.GetDefaultChannel(PipelineStopToken).ConfigureAwait(false);

        var job = new ConsumerJob(channel, Queue)
        {
            UnregisterAfterCount = UnregisterAfterCount,
            UnregisterAfterWindow = UnregisterAfterWindow,
        };
        JobRepository.Add(job);
        await WriteObjectAsync(job).ConfigureAwait(false);
        _ = job.StartJobAsync();
    }
}
