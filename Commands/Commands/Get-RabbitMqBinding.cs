using System.Management.Automation;
using PSSharp.PowerShell.RabbitMQ.Commands.Management;

namespace PSSharp.PowerShell.RabbitMQ.Commands;

[Cmdlet(
    VerbsCommon.Get,
    "RabbitMqBinding",
    RemotingCapability = RemotingCapability.OwnedByCommand,
    DefaultParameterSetName = VHostParameterSet
)]
public sealed class GetRabbitMqBindingCommand : RmqCmdlet
{
    private const string VHostParameterSet = "VHost";
    private const string AllVHostsParameterSet = "AllVhosts";

    [Parameter]
    public string? Source { get; set; }

    [Parameter]
    public string? Destination { get; set; }

    [Parameter]
    public string? RoutingKey { get; set; }

    [Parameter]
    public string? PropertiesKey { get; set; }

    [Parameter]
    public DestinationType? DestinationType { get; init; }

    [Parameter(Mandatory = true)]
    [ManagementServerUriCompleter]
    [ValidateAbsoluteUri]
    [ValidateUriScheme("http", "https")]
    public Uri? ManagementUri { get; set; }

    [Parameter]
    [Credential]
    [ArgumentCompletions("guest")]
    public PSCredential? Credential { get; set; }

    [Parameter(ParameterSetName = VHostParameterSet, ValueFromPipelineByPropertyName = true)]
    [Alias("VirtualHost")]
    [ValidateNotNullOrEmpty]
    [VirtualHostCompleter]
    public string? VHost { get; set; } = "/";

    [Parameter(ParameterSetName = AllVHostsParameterSet)]
    public SwitchParameter AllVhosts
    {
        get => VHost is null;
        set => VHost = value ? null : "/";
    }

    protected override async Task ProcessRecordAsync()
    {
        Assert.MandatoryParameter(ManagementUri, "All");
        var cred = Credential?.GetNetworkCredential() ?? ManagementUri.GetNetworkCredential();
        if (cred is null)
        {
            await this.ManagementCredentialRequiredError(ManagementUri);
            return;
        }

        using var client = new ManagementClient(ManagementUri.WithoutCredential(), cred);

        await foreach (
            var binding in client.ListBindings(
                VHost,
                Source,
                Destination,
                DestinationType,
                RoutingKey,
                PropertiesKey,
                PipelineStopToken
            )
        )
        {
            await WriteObjectAsync(binding);
        }
    }
}
