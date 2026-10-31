using RabbitMQ.Client;
using System.Collections;
using System.Diagnostics;
using System.Management.Automation;

namespace PSSharp.PowerShell.RabbitMQ;

[Cmdlet(VerbsCommon.New, Noun, ConfirmImpact = ConfirmImpact.Low, DefaultParameterSetName = UriParameterSet, RemotingCapability = RemotingCapability.OwnedByCommand, SupportsShouldProcess = true)]
public sealed class NewRabbitMqConnectionCommand : RmqCmdlet
{
    public const string Noun = "RabbitMqConnection";
    public const string UriParameterSet = "Uri";
    public const string ArgsParameterSet = "Arg";
    public const string FactoryParameterSet = "Factory";

    [Parameter(ParameterSetName = UriParameterSet, Mandatory = true, Position = 0, ValueFromPipeline = true)]
    public Uri? Uri { get; set; }

    [Parameter(ParameterSetName = FactoryParameterSet, Mandatory = true, Position = 0, ValueFromPipeline = true)]
    public ConnectionFactory? Factory { get; set; }

    [Parameter(ParameterSetName = ArgsParameterSet, Mandatory = true, Position = 0, ValueFromPipelineByPropertyName = true)]
    public string? Hostname { get; set; }

    [Parameter(ParameterSetName = ArgsParameterSet, Position = 1, ValueFromPipelineByPropertyName = true)]
    public ushort Port { get; set; } = 5672;

    [Parameter(ParameterSetName = ArgsParameterSet, Position = 2, ValueFromPipelineByPropertyName = true)]
    public string VirtualHost { get; set; } = "/";

    /// <summary>
    /// If credentials are not provided, "guest:guest" will be used.
    /// </summary>
    [Parameter(ParameterSetName = ArgsParameterSet, Position = 3)]
    public PSCredential? Credential { get; set; }

    [Parameter(ParameterSetName = ArgsParameterSet)]
    public Hashtable? ClientProperties { get; set; }

    [Parameter]
    public string ClientProvidedName { get; set; } = "PSSharp.PowerShell.RabbitMQ";

    protected override async Task ProcessRecordAsync()
    {
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

    private async Task ConnectUsingFactory()
    {
        Factory!.ClientProvidedName = ClientProvidedName;
        var connection = await Factory.CreateConnectionAsync(StoppingToken);
        var session = new PSRabbitConnection(connection);
        new RabbitConnectionStore(SessionState.PSVariable).Add(session);
        await WriteObjectAsync(session);
    }

    private async Task ConnectUsingArgs()
    {
        var factory = new ConnectionFactory()
        {
            HostName = Hostname!,
            ClientProvidedName = ClientProvidedName,
            UserName = Credential?.UserName ?? "guest",
            Password = Credential?.GetNetworkCredential().Password ?? "guest",
            ClientProperties = ClientProperties?.Cast<DictionaryEntry>().ToDictionary(e => e.Key.ToString()!, e => e.Value) ?? [],
            Port = Port,
            VirtualHost = VirtualHost,
        };
        var connection = await factory.CreateConnectionAsync(StoppingToken);
        var session = new PSRabbitConnection(connection);
        new RabbitConnectionStore(SessionState.PSVariable).Add(session);
        await WriteObjectAsync(session);
    }

    private async Task ConnectUsingUri()
    {
        var factory = new ConnectionFactory
        {
            Uri = Uri!,
            ClientProvidedName = ClientProvidedName
        };
        var connection = await factory.CreateConnectionAsync(StoppingToken);
        var session = new PSRabbitConnection(connection);
        new RabbitConnectionStore(SessionState.PSVariable).Add(session);
        await WriteObjectAsync(session);
    }
}
