using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Management.Automation;

namespace PSSharp.PowerShell.RabbitMQ;

public sealed class ConsumerJob : Job
{
    private readonly CancellationTokenSource _cts = new();
    private readonly IChannel _channel;
    private readonly string _queueName;
    private string? _consumerTag;

    public ulong UnregisterAfterCount { get; init; } = ulong.MaxValue;
    private ulong _receivedCount;

    public ConsumerJob(IChannel channel, string queueName)
    {
        _channel = channel;
        _queueName = queueName;
        PSJobTypeName = nameof(ConsumerJob);
    }

    public override string StatusMessage => string.Empty;

    public override bool HasMoreData => Output.Count > 0 || Error.Count > 0 || Progress.Count > 0 || Warning.Count > 0 || Verbose.Count > 0 || Debug.Count > 0;

    public override string Location => _queueName;

    public async Task StartJobAsync()
    {
        var consumer = new AsyncEventingBasicConsumer(_channel);

        consumer.RegisteredAsync += (sender, e) =>
        {
            Debug.Add(new DebugRecord("Consumer RegisteredAsync at " + DateTime.Now.ToString("o")));
            SetJobState(JobState.Running);
            return Task.CompletedTask;
        };

        consumer.ReceivedAsync += async (sender, e) =>
        {
            Output.Add(PSObject.AsPSObject(e));

            if (Interlocked.Increment(ref _receivedCount) == UnregisterAfterCount)
            {
                await _channel.BasicCancelAsync(e.ConsumerTag, false, _cts.Token).ConfigureAwait(false);
            }
        };

        consumer.UnregisteredAsync += (sender, e) =>
        {
            Debug.Add(new DebugRecord("Consumer UnregisteredAsync at " + DateTime.Now.ToString("o")));
            if (JobStateInfo.State == JobState.Stopping)
            {
                SetJobState(JobState.Stopped);
            }
            else if (_receivedCount >= UnregisterAfterCount)
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
                var er = new ErrorRecord(e.Exception, "ChannelShutdownError", ErrorCategory.ConnectionError, e);
                Error.Add(er);
            }
            else
            {
                var ex = new Exception("The channel was shut down.");
                var er = new ErrorRecord(ex, "ChannelShutdownError", ErrorCategory.ConnectionError, e);
                Error.Add(er);
            }
            SetJobState(JobState.Failed);
            return Task.CompletedTask;
        };

        try
        {
            _consumerTag = await _channel.BasicConsumeAsync(_queueName, true, consumer, _cts.Token);
        }
        catch (Exception e)
        {
            Error.Add(new ErrorRecord(e, "BasicConsumeError", ErrorCategory.NotSpecified, _queueName));
            SetJobState(JobState.Failed);
        }
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
                var er = new ErrorRecord(e, "BasicCancelError", ErrorCategory.NotSpecified, _consumerTag);
                Error.Add(er);
            }
        }
        else
        {
            SetJobState(JobState.Stopped);
        }
    }
}
