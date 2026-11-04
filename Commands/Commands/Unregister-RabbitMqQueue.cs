using System.Diagnostics;
using System.Management.Automation;
using PSSharp.PowerShell.RabbitMQ.Commands.Management;

namespace PSSharp.PowerShell.RabbitMQ.Commands;

[Cmdlet(
    VerbsLifecycle.Unregister,
    Noun,
    SupportsShouldProcess = true,
    DefaultParameterSetName = AmqpParameterSet
)]
public sealed class UnregisterRabbitMqQueueCommand : RmqCmdlet
{
    private const string Noun = "RabbitMqQueue";
    private const string AmqpParameterSet = "Amqp";
    private const string ManagementParameterSet = "Management";

    [Parameter(Mandatory = true, ValueFromPipelineByPropertyName = true)]
    [QueueNameCompleter]
    [Alias("QueueName")]
    public string Queue { get; set; } = string.Empty;

    [Parameter]
    public SwitchParameter IfUnused { get; set; }

    [Parameter]
    public SwitchParameter IfEmpty { get; set; }

    [Parameter(Mandatory = true, ParameterSetName = AmqpParameterSet)]
    [RabbitConnection]
    [RabbitConnectionCompleter]
    public PSRabbitConnection? Connection { get; set; }

    [Parameter(Mandatory = true, ParameterSetName = ManagementParameterSet)]
    [ManagementServerUriCompleter]
    [ValidateAbsoluteUri]
    [ValidateUriScheme("http", "https")]
    public Uri? ManagementUri { get; set; }

    [Parameter(ParameterSetName = ManagementParameterSet)]
    [ArgumentCompletions("guest")]
    public PSCredential? Credential { get; set; }

    [Parameter(ParameterSetName = ManagementParameterSet)]
    [ValidateNotNullOrEmpty]
    [ArgumentCompletions("/")]
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
                throw new UnreachableException();
        }
    }

    private async Task Amqp()
    {
        Debug.Assert(Connection is not null, "Connection is mandatory for Amqp parameter set.");

        var channel = await Connection.GetDefaultChannel(PipelineStopToken);

        if (!await InvokeAsync(() => ShouldProcess(Queue, "delete queue")))
        {
            return;
        }

        await channel.QueueDeleteAsync(Queue, IfUnused, IfEmpty, noWait: false, PipelineStopToken);
    }

    private async Task Management()
    {
        Debug.Assert(
            ManagementUri is not null,
            "ManagementUri is mandatory for Management parameter set."
        );

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
