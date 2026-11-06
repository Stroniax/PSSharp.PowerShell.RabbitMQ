using System.Diagnostics;
using System.Management.Automation;
using PSSharp.PowerShell.RabbitMQ.Commands.Management;

namespace PSSharp.PowerShell.RabbitMQ.Commands;

[Cmdlet(
    VerbsCommon.Clear,
    Nouns.RabbitMqQueue,
    SupportsShouldProcess = true,
    DefaultParameterSetName = AmqpParameterSet,
    RemotingCapability = RemotingCapability.OwnedByCommand
)]
public sealed class ClearRabbitMqQueueCommand : RmqCmdlet
{
    private const string AmqpParameterSet = "Amqp";
    private const string ManagementParameterSet = "Management";

    [Parameter(Mandatory = true, Position = 0, ValueFromPipelineByPropertyName = true)]
    [Alias("QueueName")]
    [QueueNameCompleter]
    public string Queue { get; set; } = string.Empty;

    [Parameter(Mandatory = true, ParameterSetName = AmqpParameterSet)]
    [Alias("Connection")]
    [PSRabbitSession]
    [PSRabbitSessionCompleter]
    public PSRabbitSession? Session { get; set; }

    [Parameter(Mandatory = true, ParameterSetName = ManagementParameterSet)]
    [ManagementServerUriCompleter]
    [ValidateAbsoluteUri]
    [ValidateUriScheme("http", "https")]
    public Uri? ManagementUri { get; set; }

    [Parameter(ParameterSetName = ManagementParameterSet)]
    [Credential]
    [ArgumentCompletions("guest")]
    public PSCredential? Credential { get; set; }

    [Parameter(ParameterSetName = ManagementParameterSet, ValueFromPipelineByPropertyName = true)]
    [Alias("VirtualHost")]
    [ValidateNotNullOrEmpty]
    [VirtualHostCompleter]
    public string VHost { get; set; } = "/";

    protected override async Task ProcessRecordAsync()
    {
        switch (ParameterSetName)
        {
            case AmqpParameterSet:
                await Amqp();
                return;
            case ManagementParameterSet:
                await Management();
                return;
            default:
                Assert.ParameterSetExhausted(ParameterSetName);
                return;
        }
    }

    private async Task Amqp()
    {
        Assert.MandatoryParameter(Session, AmqpParameterSet);

        var channel = await Session.GetDefaultChannel(PipelineStopToken);

        if (!await InvokeAsync(() => ShouldProcess(Queue, "purge queue")))
        {
            return;
        }

        await channel.QueuePurgeAsync(Queue, PipelineStopToken);
    }

    private async Task Management()
    {
        Assert.MandatoryParameter(ManagementUri, ManagementParameterSet);

        var cred = Credential?.GetNetworkCredential() ?? ManagementUri.GetNetworkCredential();

        if (cred is null)
        {
            await this.ManagementCredentialRequiredError(ManagementUri);
            return;
        }

        using var client = new ManagementClient(ManagementUri.WithoutCredential(), cred);

        if (!await InvokeAsync(() => ShouldProcess(Queue, "purge queue")))
        {
            return;
        }

        await client.PurgeQueue(VHost, Queue, PipelineStopToken);
    }
}
