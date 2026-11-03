using System.Management.Automation;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace PSSharp.PowerShell.RabbitMQ.Commands;

/// <remarks>
/// Because the job owns the <see cref="IChannel"/>, removing the job (<c>Remove-Job</c>) which calls
/// <see cref="Job.Dispose"/> will release the channel and prevent any pending messages from being
/// acked or nacked. For this reason, stop the job at will but do not remove the job until the messages
/// are fully processed.
/// </remarks>
public sealed class ConsumerJob : Job
{
    private readonly CancellationTokenSource _cts = new();
    private readonly IChannel _channel;
    private readonly string _queueName;
    private string? _consumerTag;
    private WindowWatcher? _windowWatcher;

    public ulong UnregisterAfterCount { get; init; } = ulong.MaxValue;
    public TimeSpan UnregisterAfterWindow { get; init; } =
        TimeSpan.FromMilliseconds(uint.MaxValue - 1);
    public bool AutoAck { get; init; }

    /// <summary>
    /// How many messages to queue ('in-flight') in the job before waiting for the messages to be processed.
    /// In auto-ack mode, this has virtually no effect. In manual-ack mode, this will prevent more than this
    /// number of messages from sitting in the job until some of the messages have been processed. For batching,
    /// it is necessary to set this to a higher number.
    /// </summary>
    public ushort PrefetchCount { get; init; } = 1;

    private ulong _receivedCount;

    /// <param name="channel">Owned by this job. Disposed when the job is disposed.</param>
    public ConsumerJob(IChannel channel, string queueName)
    {
        _channel = channel;
        _queueName = queueName;
        PSJobTypeName = nameof(ConsumerJob);
    }

    public override string StatusMessage => string.Empty;

    public override bool HasMoreData =>
        Output.Count > 0
        || Error.Count > 0
        || Progress.Count > 0
        || Warning.Count > 0
        || Verbose.Count > 0
        || Debug.Count > 0;

    public override string Location => _queueName;

    public async Task StartJobAsync()
    {
        _windowWatcher = new WindowWatcher(UnregisterAfterWindow);
        var consumer = new AsyncEventingBasicConsumer(_channel);

        consumer.RegisteredAsync += (sender, e) =>
        {
            Debug.Add(new DebugRecord("Consumer RegisteredAsync at " + DateTime.Now.ToString("o")));
            SetJobState(JobState.Running);

            // First notification so that the timer starts ticking
            _windowWatcher?.Reset();

            return Task.CompletedTask;
        };

        consumer.ReceivedAsync += async (sender, e) =>
        {
            _windowWatcher?.Reset();

            if (!PSRabbitMessage.FromDeliver(_channel, e, null, out var message, out var error))
            {
                Error.Add(error);
            }
            Output.Add(new PSObject(message));

            if (Interlocked.Increment(ref _receivedCount) == UnregisterAfterCount)
            {
                await _channel
                    .BasicCancelAsync(e.ConsumerTag, false, _cts.Token)
                    .ConfigureAwait(false);
            }
        };

        consumer.UnregisteredAsync += (sender, e) =>
        {
            Debug.Add(
                new DebugRecord("Consumer UnregisteredAsync at " + DateTime.Now.ToString("o"))
            );
            if (JobStateInfo.State == JobState.Stopping)
            {
                SetJobState(JobState.Stopped);
            }
            else if (
                _receivedCount >= UnregisterAfterCount
                || _windowWatcher is { IsFinished: true }
            )
            {
                SetJobState(JobState.Completed);
            }
            else
            {
                // If it isn't already stopping, it was unregistered by another reason (ShutdownAsync) that will be called next.
                //SetJobState(JobState.Completed);
            }
            return Task.CompletedTask;
        };

        consumer.ShutdownAsync += (sender, e) =>
        {
            Debug.Add(new DebugRecord("Consumer ShutdownAsync at " + DateTime.Now.ToString("o")));
            if (e.Exception is not null)
            {
                var er = new ErrorRecord(
                    e.Exception,
                    "ChannelShutdownError",
                    ErrorCategory.ConnectionError,
                    e
                );
                Error.Add(er);
            }
            else
            {
                var ex = new Exception("The channel was shut down.");
                var er = new ErrorRecord(
                    ex,
                    "ChannelShutdownError",
                    ErrorCategory.ConnectionError,
                    e
                );
                Error.Add(er);
            }
            SetJobState(JobState.Failed);
            return Task.CompletedTask;
        };

        try
        {
            await _channel.BasicQosAsync(0, PrefetchCount, false, _cts.Token);
            _consumerTag = await _channel.BasicConsumeAsync(
                _queueName,
                AutoAck,
                consumer,
                _cts.Token
            );
        }
        catch (Exception e)
        {
            Error.Add(
                new ErrorRecord(e, "BasicConsumeError", ErrorCategory.NotSpecified, _queueName)
            );
            SetJobState(JobState.Failed);
            return;
        }

        _ = Task.Run(async () =>
        {
            await _windowWatcher.WaitAsync(_cts.Token);
            if (JobStateInfo.State == JobState.Running)
            {
                Debug.Add(
                    new DebugRecord(
                        "The 'UnregisterAfterWindow' interval has elapsed. Canceling the consumer."
                    )
                );
                await _channel.BasicCancelAsync(_consumerTag, false, _cts.Token);
            }
        });
    }

    public override async void StopJob()
    {
        _cts.CancelAfter(TimeSpan.FromSeconds(30));
        SetJobState(JobState.Stopping);

        if (_consumerTag is not null)
        {
            try
            {
                await _channel.BasicCancelAsync(_consumerTag, false, _cts.Token);
            }
            catch (Exception e)
            {
                var er = new ErrorRecord(
                    e,
                    "BasicCancelError",
                    ErrorCategory.NotSpecified,
                    _consumerTag
                );
                Error.Add(er);
            }
        }
        else
        {
            SetJobState(JobState.Stopped);
        }
    }

    protected override void Dispose(bool disposing)
    {
        base.Dispose(disposing);
        if (disposing)
        {
            _channel.Dispose();
            _windowWatcher?.Dispose();
            _cts?.Dispose();
        }
    }
}
