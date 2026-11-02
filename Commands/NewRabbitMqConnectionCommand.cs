using System.Collections;
using System.Diagnostics;
using System.Management.Automation;
using RabbitMQ.Client;

namespace PSSharp.PowerShell.RabbitMQ.Commands;

[Cmdlet(
    VerbsCommon.New,
    Noun,
    ConfirmImpact = ConfirmImpact.Low,
    DefaultParameterSetName = UriParameterSet,
    RemotingCapability = RemotingCapability.OwnedByCommand,
    SupportsShouldProcess = true
)]
public sealed class NewRabbitMqConnectionCommand : RmqCmdlet
{
    public const string Noun = "RabbitMqConnection";
    public const string UriParameterSet = "Uri";
    public const string ArgsParameterSet = "Arg";
    public const string FactoryParameterSet = "Factory";

    [Parameter(
        ParameterSetName = UriParameterSet,
        Mandatory = true,
        Position = 0,
        ValueFromPipeline = true
    )]
    [ValidateAbsoluteUri]
    [ValidateUriScheme("amqp", "amqps")]
    public Uri? Uri { get; set; }

    [Parameter(
        ParameterSetName = FactoryParameterSet,
        Mandatory = true,
        Position = 0,
        ValueFromPipeline = true
    )]
    public ConnectionFactory? Factory { get; set; }

    [Parameter(
        ParameterSetName = ArgsParameterSet,
        Mandatory = true,
        Position = 0,
        ValueFromPipelineByPropertyName = true
    )]
    public string? Hostname { get; set; }

    [Parameter(
        ParameterSetName = ArgsParameterSet,
        Position = 1,
        ValueFromPipelineByPropertyName = true
    )]
    public ushort Port { get; set; } = 5672;

    [Parameter(
        ParameterSetName = ArgsParameterSet,
        Position = 2,
        ValueFromPipelineByPropertyName = true
    )]
    public string VirtualHost { get; set; } = "/";

    /// <summary>
    /// If credentials are not provided, "guest:guest" will be used.
    /// </summary>
    [Parameter(ParameterSetName = ArgsParameterSet, Position = 3)]
    [Parameter(ParameterSetName = UriParameterSet, Position = 1)]
    [Credential]
    public PSCredential? Credential { get; set; }

    [Parameter(ParameterSetName = ArgsParameterSet)]
    public Hashtable? ClientProperties { get; set; }

    [Parameter]
    public string ClientProvidedName { get; set; } = "PSSharp.PowerShell.RabbitMQ";

    /// <summary>
    /// A friendly alias to identify the connection. Must be unique within this PowerShell session.
    /// </summary>
    [Parameter]
    [ValidateNotNullOrWhiteSpace]
    public string? Name { get; set; }

    private readonly RabbitConnectionStore _store = RabbitConnectionStore.Current;

    protected override async Task ProcessRecordAsync()
    {
        if (Name is not null && _store.HasSessionWithName(Name))
        {
            await WriteErrorNameExists();
            return;
        }

        switch (ParameterSetName)
        {
            case UriParameterSet:
                await ConnectUsingUri();
                break;
            case ArgsParameterSet:
                await ConnectUsingArgs();
                break;
            case FactoryParameterSet:
                await ConnectUsingFactory();
                break;
            default:
                await WriteErrorAsync(
                    new ErrorRecord(
                        new UnreachableException("The parameter set is not implemented."),
                        "ParameterSetNotImplemented",
                        ErrorCategory.NotSpecified,
                        ParameterSetName
                    )
                );
                break;
        }
    }

    private async Task WriteErrorNameExists(string? name = null)
    {
        var isAutoName = Name is null;
        name ??= Name;
        var autoMessage = " The name was auto-generated from the host name of the connection.";

        var ex = new InvalidOperationException(
            $"The name (alias) is associated with an existing RabbitMQ session.{autoMessage}"
        );
        var er = new ErrorRecord(ex, "NameExists", ErrorCategory.InvalidArgument, Name)
        {
            ErrorDetails = new ErrorDetails(
                $"The name (alias) \"{name}\" is associated with an existing RabbitMQ session.{autoMessage}"
            )
            {
                RecommendedAction =
                    "Specify a different name using the \"-Name\" parameter, or remove the existing RabbitMQ connection using \"Remove-RabbitMqConnection\".",
            },
        };
        await WriteErrorAsync(er);
    }

    private async Task ConnectUsingFactory()
    {
        Debug.Assert(
            Factory is not null,
            "The parameter set that leads to ConnectUsingFactory to be called requires that the Factory is provided and not null."
        );

        if (MyInvocation.BoundParameters.ContainsKey(nameof(ClientProvidedName)))
        {
            Factory.ClientProvidedName ??= ClientProvidedName;
        }
        await CreateConnection(Factory);
    }

    private async Task ConnectUsingArgs()
    {
        var factory = new ConnectionFactory()
        {
            HostName = Hostname!,
            ClientProvidedName = ClientProvidedName,
            UserName = Credential?.UserName ?? "guest",
            Password = Credential?.GetNetworkCredential().Password ?? "guest",
            ClientProperties =
                ClientProperties
                    ?.Cast<DictionaryEntry>()
                    .ToDictionary(e => e.Key.ToString()!, e => e.Value)
                ?? [],
            Port = Port,
            VirtualHost = VirtualHost,
        };
        await CreateConnection(factory);
    }

    private async Task ConnectUsingUri()
    {
        var factory = new ConnectionFactory { Uri = Uri!, ClientProvidedName = ClientProvidedName };
        if (Credential is not null)
        {
            factory.UserName = Credential.UserName;
            factory.Password = Credential.GetNetworkCredential().Password;
        }
        await CreateConnection(factory);
    }

    private async Task CreateConnection(ConnectionFactory factory)
    {
        var name = Name ?? factory.HostName;
        if (_store.HasSessionWithName(name))
        {
            await WriteErrorNameExists(name).ConfigureAwait(false);
            return;
        }

        if (!await ConfirmConnect(factory).ConfigureAwait(false))
        {
            return;
        }

        await ProgressConnecting(factory).ConfigureAwait(false);

        IConnection connection;
        try
        {
            connection = await factory
                .CreateConnectionAsync(PipelineStopToken)
                .ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            var er = new ErrorRecord(ex, "ConnectionError", ErrorCategory.ConnectionError, factory);
            await WriteErrorAsync(er).ConfigureAwait(false);
            return;
        }
        finally
        {
            await ProgressCompleted().ConfigureAwait(false);
        }

        var session = new PSRabbitConnection(connection) { Name = name };
        _store.Add(session);
        await WriteObjectAsync(session).ConfigureAwait(false);
    }

    private Task<bool> ConfirmConnect(ConnectionFactory factory)
    {
        return InvokeAsync(() => ShouldProcess(factory.HostName, "connect to RabbitMQ server"));
    }

    private Task ProgressConnecting(ConnectionFactory factory)
    {
        return InvokeAsync(() =>
            WriteProgress(
                new(
                    1,
                    "Connecting to RabbitMQ server",
                    $"Attempting connection to {factory.HostName}..."
                )
            )
        );
    }

    private Task ProgressCompleted()
    {
        return InvokeAsync(() =>
            WriteProgress(new(1) { RecordType = ProgressRecordType.Completed })
        );
    }
}
