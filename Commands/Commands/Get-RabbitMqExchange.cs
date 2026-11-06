using System.Management.Automation;
using PSSharp.PowerShell.RabbitMQ.Commands.Management;

namespace PSSharp.PowerShell.RabbitMQ.Commands;

[Cmdlet(
    VerbsCommon.Get,
    Nouns.RabbitMqExchange,
    RemotingCapability = RemotingCapability.OwnedByCommand,
    DefaultParameterSetName = VhostParameterSet
)]
public sealed class GetRabbitMqExchangeCommand : RmqCmdlet
{
    private const string VhostParameterSet = "Vhost";
    private const string AllVhostsParameterSet = "AllVirtualHosts";

    [Parameter(Position = 0, ValueFromPipelineByPropertyName = true)]
    [SupportsWildcards]
    [Alias("ExchangeName", "Name")]
    [ExchangeNameCompleter]
    public string[] Exchange { get; set; } = [];

    [Parameter(Mandatory = true)]
    [ValidateAbsoluteUri]
    [ValidateUriScheme("http", "https")]
    [ManagementServerUriCompleter]
    public required Uri ManagementUri { get; set; }

    [Parameter]
    [ArgumentCompletions("guest")]
    [Credential]
    public PSCredential? Credential { get; set; }

    [Parameter(
        Position = 1,
        ParameterSetName = VhostParameterSet,
        ValueFromPipelineByPropertyName = true
    )]
    [Alias("VirtualHost")]
    [ValidateNotNullOrEmpty]
    [VirtualHostCompleter]
    public string VHost { get; set; } = "/";

    [Parameter(ParameterSetName = AllVhostsParameterSet)]
    public SwitchParameter AllVirtualHosts { get; set; }

    protected override async Task ProcessRecordAsync()
    {
        var hasWildcards = Exchange.Any(WildcardPattern.ContainsWildcardCharacters);

        var cred = Credential?.GetNetworkCredential() ?? ManagementUri.GetNetworkCredential();
        if (cred is null)
        {
            await this.ManagementCredentialRequiredError(ManagementUri);
            return;
        }

        using var client = new ManagementClient(ManagementUri.WithoutCredential(), cred);

        // even if there are some or many exchanges with exact names given, any wildcard means we
        // have to list anyway so just find the exact matches in the direct list.
        if (hasWildcards || AllVirtualHosts || Exchange.Length == 0)
        {
            await ListBySearch(client);
        }
        else
        {
            await FindExact(client);
        }
    }

    private async Task FindExact(ManagementClient client)
    {
        foreach (var name in Exchange)
        {
            var exch = await client.GetExchange(VHost, name, PipelineStopToken);
            if (exch is null)
            {
                var er = Assert.ItemNotFound(
                    "Exchange",
                    exch,
                    nameof(ExchangeDetails.Name),
                    $" on virtual host \"{VHost}\""
                );
                await WriteErrorAsync(er);
            }
            else
            {
                await WriteObjectAsync(exch);
            }
        }
    }

    private async Task ListBySearch(ManagementClient client)
    {
        var wildcards = Exchange
            .Where(WildcardPattern.ContainsWildcardCharacters)
            .Select(w => new WildcardPattern(w, WildcardOptions.IgnoreCase))
            .ToArray();
        var exacts = Exchange
            .Where(e => !WildcardPattern.ContainsWildcardCharacters(e))
            .ToHashSet(StringComparer.OrdinalIgnoreCase);
        var foundExacts = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        var vhost = AllVirtualHosts ? null : VHost;
        await foreach (var exch in client.ListExchanges(vhost, PipelineStopToken))
        {
            if (Exchange.Length == 0)
            {
                await WriteObjectAsync(exch);
                continue;
            }
            else if (exacts.Contains(exch.Name))
            {
                await WriteObjectAsync(exch);
                foundExacts.Add(exch.Name);
                continue;
            }
            else if (wildcards.Any(w => w.IsMatch(exch.Name)))
            {
                await WriteObjectAsync(exch);
                continue;
            }
        }

        foreach (var name in exacts)
        {
            if (foundExacts.Contains(name))
            {
                continue;
            }

            var er = Assert.ItemNotFound(
                "Exchange",
                name,
                nameof(ExchangeDetails.Name),
                " matching the provided criteria"
            );
            await WriteErrorAsync(er);
        }
    }
}
