using System.Diagnostics;
using System.Management.Automation;
using PSSharp.PowerShell.RabbitMQ.Commands.Management;

namespace PSSharp.PowerShell.RabbitMQ.Commands;

/// <summary>
/// Registers a RabbitMQ management server. This server can be used for auto-completion and some limited
/// amount of other stuff probably.
/// </summary>
[Cmdlet(VerbsLifecycle.Register, Noun)]
public sealed class RegisterRabbitMqManagementServerCommand : RmqCmdlet
{
    private const string Noun = "RabbitMqManagementServer";
    private const string UriParameterSet = "Uri";

    [Parameter(ParameterSetName = UriParameterSet, Mandatory = true)]
    [ManagementServerUriCompleter]
    public Uri? ManagementUri { get; set; }

    [Parameter]
    [Credential]
    [ArgumentCompletions("guest")]
    public PSCredential? Credential { get; set; }

    [Parameter]
    public SwitchParameter PassThru { get; set; }

    protected override async Task ProcessRecordAsync()
    {
        Debug.Assert(ManagementUri is not null, "Mandatory parameter 'Uri' cannot be null.");

        using var client = new HttpClient();

        var cred = Credential?.GetNetworkCredential() ?? ManagementUri.GetNetworkCredential();

        if (cred is null)
        {
            await this.ManagementCredentialRequiredError(ManagementUri);
            return;
        }

        var server = new RmqManagementServer(
            ManagementUri,
            new ManagementClient(ManagementUri.WithoutCredential(), cred)
        );

        RmqManagementServer.AddOrUpdate(server);

        if (PassThru)
        {
            await WriteObjectAsync(server);
        }
    }
}
