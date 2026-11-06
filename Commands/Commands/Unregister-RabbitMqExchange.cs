using System.Diagnostics;
using System.Management.Automation;
using PSSharp.PowerShell.RabbitMQ.Commands.Management;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;

namespace PSSharp.PowerShell.RabbitMQ.Commands;

[Cmdlet(
    VerbsLifecycle.Unregister,
    Nouns.RabbitMqExchange,
    DefaultParameterSetName = AmqpParameterSet,
    RemotingCapability = RemotingCapability.OwnedByCommand,
    SupportsShouldProcess = true
)]
public sealed class UnregisterRabbitMqExchangeCommand : RmqCmdlet
{
    private const string AmqpParameterSet = "Amqp";
    private const string ManagementParameterSet = "Management";

    [Parameter(Mandatory = true, Position = 0, ValueFromPipelineByPropertyName = true)]
    [Alias("ExchangeName", "Name")]
    [ExchangeNameCompleter]
    public string Exchange { get; set; } = string.Empty;

    [Parameter]
    public SwitchParameter IfUnused { get; set; }

    [Parameter(Mandatory = true, ParameterSetName = AmqpParameterSet)]
    [Alias("Connection")]
    [PSRabbitSession]
    [PSRabbitSessionCompleter]
    public PSRabbitSession? Session { get; set; }

    [Parameter(Mandatory = true, ParameterSetName = ManagementParameterSet)]
    [ManagementServerUriCompleter]
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
                break;
            case ManagementParameterSet:
                await Management();
                break;
            default:
                Assert.ParameterSetExhausted(ParameterSetName);
                break;
        }
    }

    private async Task Amqp()
    {
        Assert.MandatoryParameter(Session, AmqpParameterSet);

        var channel = await Session.GetDefaultChannel(PipelineStopToken);

        if (!await InvokeAsync(() => ShouldProcess(Exchange, "delete exchange")))
        {
            return;
        }

        try
        {
            await channel.ExchangeDeleteAsync(Exchange, IfUnused, noWait: false, PipelineStopToken);
        }
        catch (OperationInterruptedException ex)
            when (ex.ShutdownReason is { ReplyCode: Constants.PreconditionFailed })
        {
            var er = new ErrorRecord(ex, "ExchangeInUse", ErrorCategory.ResourceBusy, Exchange);
            await WriteErrorAsync(er);
            return;
        }
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

        if (!await InvokeAsync(() => ShouldProcess(Exchange, "delete exchange")))
        {
            return;
        }

        bool deleted;
        try
        {
            deleted = await client.DeleteExchange(
                VHost,
                Exchange,
                new() { IfUnused = IfUnused },
                PipelineStopToken
            );
        }
        catch (InvalidOperationException ex)
            when (ex.Data.Contains("StatusCode")
                && ex.Data["StatusCode"] is 400
                && ex.Message.Contains("exchange")
                && ex.Message.Contains("in use")
            )
        {
            var er = new ErrorRecord(ex, "ExchangeInUse", ErrorCategory.ResourceBusy, Exchange);
            await WriteErrorAsync(er);
            return;
        }

        if (!deleted)
        {
            await InvokeAsync(() => WriteDebug($"The exchange \"{Exchange}\" was not found."));
        }
    }
}
