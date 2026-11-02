using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Collections.Generic;
using System.Management.Automation;
using System.Text;

namespace PSSharp.PowerShell.RabbitMQ.Commands;

[Cmdlet(VerbsLifecycle.Start, Noun)]
public sealed class StartRabbitMqConsumerCommand : RmqCmdlet
{
    public const string Noun = "RabbitMqConsumer";

    [Parameter(Mandatory = true, Position = 0, ValueFromPipelineByPropertyName = true)]
    public string QueueName { get; set; } = string.Empty;

    [Parameter]
    [Alias("Blocking")]
    public SwitchParameter Wait { get; set; }

    /// <summary>
    /// Unregister the consuemr after this number of messages have been processed. The consumer may continue
    /// running after this number of messages have been processed if there were messages in-flight.
    /// </summary>
    [Parameter]
    [ValidateRange(1ul, ulong.MaxValue)]
    [Alias("MaxCount")]
    public ulong UnregisterAfterCount { get; set; } = ulong.MaxValue;

    [Parameter(Mandatory = true)]
    [RabbitConnectionCompleter]
    [RabbitConnection]
    public PSRabbitConnection Connection { get; set; } = null!;

    protected override async Task ProcessRecordAsync()
    {
        var channel = await Connection.GetDefaultChannel(StoppingToken).ConfigureAwait(false);

        if (Wait)
        {
            await ConsumeBlocking(channel).ConfigureAwait(false);
        }
        else
        {
            var job = new ConsumerJob(channel, QueueName)
            {
                UnregisterAfterCount = UnregisterAfterCount
            };
            JobRepository.Add(job);
            await WriteObjectAsync(job).ConfigureAwait(false);
            _ = job.StartJobAsync();
        }
    }

    private async Task ConsumeBlocking(IChannel channel)
    {
        var consumer = new AsyncEventingBasicConsumer(channel);
        string? consumerTag = null;
        var count = 0ul;
        var done = new TaskCompletionSource();
        consumer.ReceivedAsync += async (sender, args) =>
        {
            var body = args.Body.ToArray();
            var message = Encoding.UTF8.GetString(body);
            await WriteObjectAsync(message);
            if (Interlocked.Increment(ref count) == UnregisterAfterCount)
            {
                await channel.BasicCancelAsync(consumerTag!, false, StoppingToken).ConfigureAwait(false);
            }
        };
        consumer.UnregisteredAsync += (sender, e) =>
        {
            done.TrySetResult();
            return Task.CompletedTask;
        };
        consumerTag = await channel.BasicConsumeAsync(QueueName, true, consumer, StoppingToken).ConfigureAwait(false);
        await done.Task.ConfigureAwait(false);
    }
}
