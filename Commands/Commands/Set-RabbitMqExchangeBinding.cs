using System.Collections;
using System.Management.Automation;
using PSSharp.PowerShell.RabbitMQ.Commands.Management;

namespace PSSharp.PowerShell.RabbitMQ.Commands;

[Cmdlet(
    VerbsCommon.Set,
    Nouns.RabbitMqExchangeBinding,
    RemotingCapability = RemotingCapability.OwnedByCommand,
    SupportsShouldProcess = true,
    DefaultParameterSetName = AmqpParameterSet
)]
public sealed class SetRabbitMqExchangeBindingCommand : RmqCmdlet
{
    private const string AmqpParameterSet = "Amqp";
    private const string ManagementParameterSet = "Management";

    [Parameter(Mandatory = true, Position = 0, ValueFromPipelineByPropertyName = true)]
    [Alias("DestinationExchange", "ToExchange")]
    [QueueNameCompleter]
    public string Destination { get; set; } = string.Empty;

    [Parameter(Mandatory = true, Position = 1, ValueFromPipelineByPropertyName = true)]
    [Alias("SourceExchange", "FromExchange")]
    [ExchangeNameCompleter]
    public string Source { get; set; } = string.Empty;

    [Parameter(Position = 2, ValueFromPipelineByPropertyName = true, Mandatory = true)]
    [ValidateLength(0, 255)] // "must be less than 255 bytes"
    [AllowEmptyString]
    [ArgumentCompletions("''")] // TODO: add routing key completer
    public string RoutingKey { get; set; } = string.Empty;

    [Parameter(Position = 3, ValueFromPipelineByPropertyName = true)]
    public Hashtable? Arguments { get; set; }

    [Parameter(Mandatory = true, ParameterSetName = AmqpParameterSet)]
    [Alias("Connection")]
    [PSRabbitSessionCompleter]
    [PSRabbitSession]
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

    private Dictionary<string, object?>? ArgumentsDictionary()
    {
        if (Arguments is null)
        {
            return null;
        }

        var d = new Dictionary<string, object?>();
        foreach (DictionaryEntry kv in Arguments)
        {
            d.Add(LanguagePrimitives.ConvertTo<string>(kv.Key), kv.Value);
        }
        return d;
    }

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

        if (
            !await InvokeAsync(() =>
                ShouldProcess(
                    $"Source = {Source}; Destination = {Destination}; RoutingKey = {RoutingKey}",
                    "bind exchange to exchange"
                )
            )
        )
        {
            return;
        }

        await channel.ExchangeBindAsync(
            Destination,
            Source,
            RoutingKey,
            ArgumentsDictionary(),
            false,
            PipelineStopToken
        );
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

        if (
            !await InvokeAsync(() =>
                ShouldProcess(
                    $"Source = {Source}; Destination = {Destination}; RoutingKey = {RoutingKey}",
                    "bind exchange to exchange"
                )
            )
        )
        {
            return;
        }

        var found = await client.CreateExchangeBinding(
            VHost,
            Source,
            Destination,
            new() { RoutingKey = RoutingKey, Arguments = ArgumentsDictionary() ?? [] },
            PipelineStopToken
        );
    }
}
