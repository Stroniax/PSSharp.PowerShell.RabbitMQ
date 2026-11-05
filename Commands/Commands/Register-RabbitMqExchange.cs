using System.Collections;
using System.Collections.Immutable;
using System.Diagnostics;
using System.Management.Automation;
using PSSharp.PowerShell.RabbitMQ.Commands.Management;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;

namespace PSSharp.PowerShell.RabbitMQ.Commands;

[Cmdlet(
    VerbsLifecycle.Register,
    Nouns.RabbitMqExchange,
    DefaultParameterSetName = AmqpParameterSet,
    RemotingCapability = RemotingCapability.OwnedByCommand,
    SupportsShouldProcess = true
)]
public sealed class RegisterRabbitMqExchange : RmqCmdlet
{
    private const string AmqpParameterSet = "Amqp";
    private const string ManagementParameterSet = "Management";

    [Parameter(Mandatory = true, Position = 0, ValueFromPipelineByPropertyName = true)]
    [Alias("ExchangeName", "Name")]
    [ExchangeNameCompleter]
    public string Exchange { get; set; } = string.Empty;

    [Parameter(Position = 1, ValueFromPipelineByPropertyName = true)]
    [Alias("ExchangeType")]
    [ValidateSet("direct", "fanout", "topic", "headers")]
    public string Type { get; set; } = "direct";

    [Parameter(Position = 2, ValueFromPipelineByPropertyName = true)]
    public SwitchParameter Durable { get; set; }

    [Parameter(Position = 3, ValueFromPipelineByPropertyName = true)]
    public SwitchParameter AutoDelete { get; set; }

    [Parameter(ValueFromPipelineByPropertyName = true)]
    [ArgumentCompletions("@{}")]
    public Hashtable Arguments { get; set; } = [];

    [Parameter(Mandatory = true, ParameterSetName = AmqpParameterSet)]
    [RabbitConnection]
    [RabbitConnectionCompleter]
    public PSRabbitConnection? Connection { get; set; }

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

    [Parameter(ParameterSetName = ManagementParameterSet)]
    public SwitchParameter PassThru { get; set; }

    [Parameter(
        ParameterSetName = ManagementParameterSet,
        DontShow = true,
        ValueFromPipelineByPropertyName = true
    )]
    public SwitchParameter Internal { get; set; }

    private ImmutableDictionary<string, object?> ArgumentsDictionary()
    {
        var builder = ImmutableDictionary.CreateBuilder<string, object?>();

        foreach (DictionaryEntry entry in Arguments)
        {
            builder.Add(LanguagePrimitives.ConvertTo<string>(entry.Key), entry.Value);
        }

        return builder.ToImmutable();
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
                throw new UnreachableException("Parameter set is not implemented.");
        }
    }

    private async Task Amqp()
    {
        Debug.Assert(Connection is not null, "Connection parameter is mandatory.");

        var channel = await Connection.GetDefaultChannel(PipelineStopToken);

        if (!await InvokeAsync(() => ShouldProcess(Exchange, "declare exchange")))
        {
            return;
        }

        try
        {
            await channel.ExchangeDeclareAsync(
                Exchange,
                Type,
                Durable,
                AutoDelete,
                ArgumentsDictionary(),
                passive: false,
                noWait: false,
                PipelineStopToken
            );
        }
        catch (OperationInterruptedException ex)
            when (ex.ShutdownReason is { ReplyCode: Constants.PreconditionFailed })
        {
            var er = new ErrorRecord(
                ex,
                "PreconditionFailed",
                ErrorCategory.InvalidArgument,
                Exchange
            )
            {
                ErrorDetails = new(
                    "One or more arguments provided to declare the exchange do not match the preexisting arguments "
                        + "of the queue. Exchange arguments cannot be modified once the exchange has been created. "
                        + ex.ShutdownReason.ReplyText
                )
                {
                    RecommendedAction =
                        "Do not modify an exchange using this cmdlet. Either remove and recreate the exchange; or "
                        + "use this cmdlet with the same parameters so that the exchange is not modified.",
                },
            };
            await WriteErrorAsync(er);
            return;
        }
    }

    private async Task Management()
    {
        Debug.Assert(ManagementUri is not null, "ManagementUri parameter is mandatory.");

        var cred = Credential?.GetNetworkCredential() ?? ManagementUri.GetNetworkCredential();
        if (cred is null)
        {
            await this.ManagementCredentialRequiredError(ManagementUri);
            return;
        }

        using var client = new ManagementClient(ManagementUri.WithoutCredential(), cred);

        if (!await InvokeAsync(() => ShouldProcess(Exchange, "declare exchange")))
        {
            return;
        }

        await client.DeclareExchange(
            VHost,
            Exchange,
            new()
            {
                Type = Type,
                Arguments = ArgumentsDictionary(),
                AutoDelete = AutoDelete,
                Durable = Durable,
                Internal = Internal,
            },
            PipelineStopToken
        );

        if (PassThru)
        {
            var exch = await client.GetExchange(VHost, Exchange, PipelineStopToken);
            await WriteObjectAsync(exch);
        }
    }
}
