using System.Diagnostics;
using System.Management.Automation;
using System.Net.Http.Headers;
using System.Net.Http.Json;
using System.Text;
using System.Text.Json;

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
    public Uri? Uri { get; set; }

    [Parameter]
    [ValidateNotNullOrEmpty]
    [ArgumentCompletions("/")]
    public string VHost { get; set; } = "/";

    [Parameter]
    [Credential]
    [ArgumentCompletions("guest")]
    public PSCredential? Credential { get; set; }

    [Parameter]
    public SwitchParameter PassThru { get; set; }

    protected override async Task ProcessRecordAsync()
    {
        Debug.Assert(Uri is not null, "Mandatory parameter 'Uri' cannot be null.");

        using var client = new HttpClient();

        if (Credential is not null)
        {
            var bytes = Encoding.UTF8.GetBytes(
                $"{Credential.UserName}:{Credential.GetNetworkCredential().Password}"
            );

            client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue(
                "Basic",
                Convert.ToBase64String(bytes)
            );
        }

        var vhost = Uri.EscapeDataString(VHost);

        var queueUri = new Uri(Uri, $"/api/queues/{vhost}");
        var queues = await client.GetFromJsonAsync<JsonElement[]>(queueUri);

        var exchUri = new Uri(Uri, $"/api/exchanges/{vhost}");
        var exchanges = await client.GetFromJsonAsync<JsonElement[]>(exchUri);

        var server = new RmqManagementServer(Uri, VHost)
        {
            Queues = queues?.Select(q => q.GetProperty("name").GetString()!).ToList() ?? [],
            Exchanges = exchanges?.Select(e => e.GetProperty("name").GetString()!).ToList() ?? [],
        };

        RmqManagementServer.AddOrUpdate(server);

        if (PassThru)
        {
            await WriteObjectAsync(server);
        }
    }
}
