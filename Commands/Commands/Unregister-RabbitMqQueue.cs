using System.Diagnostics;
using System.Management.Automation;
using PSSharp.PowerShell.RabbitMQ.Commands.Management;

namespace PSSharp.PowerShell.RabbitMQ.Commands;

[Cmdlet(
    VerbsLifecycle.Unregister,
    Nouns.RabbitMqQueue,
    SupportsShouldProcess = true,
    DefaultParameterSetName = AmqpParameterSet
)]
public sealed class UnregisterRabbitMqQueueCommand : RmqCmdlet
{
    private const string AmqpParameterSet = "Amqp";
    private const string ManagementParameterSet = "Management";

    [Parameter(Mandatory = true, ValueFromPipelineByPropertyName = true, Position = 0)]
    [QueueNameCompleter]
    [Alias("QueueName", "Name")]
    public string Queue { get; set; } = string.Empty;

    [Parameter]
    public SwitchParameter IfUnused { get; set; }

    [Parameter]
    public SwitchParameter IfEmpty { get; set; }

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

        if (!await InvokeAsync(() => ShouldProcess(Queue, "delete queue")))
        {
            return;
        }

        await channel.QueueDeleteAsync(Queue, IfUnused, IfEmpty, noWait: false, PipelineStopToken);
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

        if (!await InvokeAsync(() => ShouldProcess(Queue, "delete queue")))
        {
            return;
        }

        var deleted = await client.DeleteQueue(
            VHost,
            Queue,
            new() { IfEmpty = IfEmpty, IfUnused = IfUnused },
            PipelineStopToken
        );

        if (!deleted)
        {
            await InvokeAsync(() => WriteDebug($"The queue '{Queue}' was not found."));
        }
    }
}
