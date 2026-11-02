using Microsoft.PowerShell.Commands;
using System.Diagnostics;
using System.Management.Automation;
using System.Text;

namespace PSSharp.PowerShell.RabbitMQ;

[Cmdlet(VerbsCommon.Get, Noun)]
public sealed class GetRabbitMqMessageCommand : RmqCmdlet
{
    private const string Noun = "RabbitMqMessage";

    [Parameter(Mandatory = true, Position = 0, ValueFromPipelineByPropertyName = true)]
    [Alias("QueueName")]
    public string Queue { get; set; } = string.Empty;

    /// <summary>
    /// Raise an error if there is no message in the queue.
    /// </summary>
    [Parameter]
    public SwitchParameter Mandatory { get; set; }

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
        var channel = await Connection.GetDefaultChannel(StoppingToken).ConfigureAwait(false);

        var autoAck = !ManualAck.IsPresent;
        var message = await channel.BasicGetAsync(Queue, autoAck, StoppingToken);

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

        var (psMessage, error) = await PSRabbitMessage.FromGet(message, Serialization, StoppingToken).ConfigureAwait(false);
        if (error is not null)
        {
            await WriteErrorAsync(error);
        }
        else
        {
            await WriteObjectAsync(psMessage);
        }
    }
}
