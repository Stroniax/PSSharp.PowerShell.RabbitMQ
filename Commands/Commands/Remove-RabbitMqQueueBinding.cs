using System.Collections;
using System.Management.Automation;
using PSSharp.PowerShell.RabbitMQ.Commands.Management;

namespace PSSharp.PowerShell.RabbitMQ.Commands;

[Cmdlet(
    VerbsCommon.Remove,
    Nouns.RabbitMqQueueBinding,
    RemotingCapability = RemotingCapability.OwnedByCommand,
    SupportsShouldProcess = true,
    DefaultParameterSetName = AmqpParameterSet
)]
public sealed class RemoveRabbitMqQueueBindingCommand : RmqCmdlet
{
    private const string AmqpParameterSet = "Amqp";
    private const string ManagementParameterSet = "Management";

    [Parameter(Mandatory = true, Position = 0, ValueFromPipelineByPropertyName = true)]
    [Alias("QueueName", "ToQueue")]
    [QueueNameCompleter]
    public string Queue { get; set; } = string.Empty;

    [Parameter(Mandatory = true, Position = 1, ValueFromPipelineByPropertyName = true)]
    [Alias("ExchangeName", "SourceExchange", "FromExchange")]
    [ExchangeNameCompleter]
    public string Exchange { get; set; } = string.Empty;

    [Parameter(
        Position = 2,
        ValueFromPipelineByPropertyName = true,
        ParameterSetName = AmqpParameterSet,
        Mandatory = true
    )]
    [ValidateLength(0, 255)] // "must be less than 255 bytes"
    [AllowEmptyString]
    [QueueNameCompleter] // todo: replace with routing key completer
    public string RoutingKey { get; set; } = string.Empty;

    [Parameter(
        Position = 3,
        ValueFromPipelineByPropertyName = true,
        ParameterSetName = AmqpParameterSet
    )]
    public Hashtable? Arguments { get; set; }

    [Parameter(
        Position = 2,
        ValueFromPipelineByPropertyName = true,
        ParameterSetName = ManagementParameterSet,
        Mandatory = true
    )]
    [QueueNameCompleter] // TODO: replace with proeprties key completer
    public string? PropertiesKey { get; init; }

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
                    $"Source = {Exchange}; Destination = {Queue}; RoutingKey = {RoutingKey}",
                    "remove exchange-to-queue binding"
                )
            )
        )
        {
            return;
        }

        await channel.QueueUnbindAsync(
            Queue,
            Exchange,
            RoutingKey,
            ArgumentsDictionary(),
            PipelineStopToken
        );
    }

    private async Task Management()
    {
        Assert.MandatoryParameter(ManagementUri, ManagementParameterSet);
        Assert.MandatoryParameter(PropertiesKey, ManagementParameterSet);
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
                    $"Source = {Exchange}; Destination = {Queue}; PropertiesKey = {PropertiesKey}",
                    "remove exchange-to-queue binding"
                )
            )
        )
        {
            return;
        }

        var found = await client.DeleteQueueBinding(
            VHost,
            Exchange,
            Queue,
            PropertiesKey,
            PipelineStopToken
        );

        if (!found)
        {
            var er = Assert.ItemNotFound("QueueBinding", PropertiesKey);
            await WriteErrorAsync(er);
        }
    }
}
