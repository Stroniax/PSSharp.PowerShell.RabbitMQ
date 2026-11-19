using System.Collections;
using System.Management.Automation;
using PSSharp.PowerShell.RabbitMQ.Commands.Management;

namespace PSSharp.PowerShell.RabbitMQ.Commands;

/// <remarks>
/// Bindings, whether from exchange to exchange or exchange to queue, are ubiquitous from the API's
/// perspective. For this reason, instead of exposing two separate cmdlets I have decided to implement
/// a single cmdlet that can retreive either binding type. I may change this later as I consider it to
/// be slightly unintuitive considered the pairing with set/remove by exchange/queue specifically.
/// </remarks>
[Cmdlet(
    VerbsCommon.Set,
    Nouns.RabbitMqQueueBinding,
    RemotingCapability = RemotingCapability.OwnedByCommand,
    SupportsShouldProcess = true,
    DefaultParameterSetName = AmqpParameterSet
)]
public sealed class SetRabbitMqQueueBindingCommand : RmqCmdlet
{
    private const string AmqpParameterSet = "Amqp";
    private const string ManagementParameterSet = "Management";

    [Parameter(Mandatory = true, Position = 0, ValueFromPipelineByPropertyName = true)]
    [Alias("QueueName", "ToQueue", "Destination")]
    [QueueNameCompleter]
    public string Queue { get; set; } = string.Empty;

    [Parameter(Mandatory = true, Position = 1, ValueFromPipelineByPropertyName = true)]
    [Alias("ExchangeName", "SourceExchange", "FromExchange", "Source")]
    [ExchangeNameCompleter]
    public string Exchange { get; set; } = string.Empty;

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
                    $"Source = {Exchange}; Destination = {Queue}; RoutingKey = {RoutingKey}",
                    "bind exchange to queue"
                )
            )
        )
        {
            return;
        }

        await channel.QueueBindAsync(
            Queue,
            Exchange,
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
                    $"Source = {Exchange}; Destination = {Queue}; RoutingKey = {RoutingKey}",
                    "bind exchange to queue"
                )
            )
        )
        {
            return;
            ;
        }

        var found = await client.CreateQueueBinding(
            VHost,
            Exchange,
            Queue,
            new() { RoutingKey = RoutingKey, Arguments = ArgumentsDictionary() ?? [] },
            PipelineStopToken
        );
    }
}
