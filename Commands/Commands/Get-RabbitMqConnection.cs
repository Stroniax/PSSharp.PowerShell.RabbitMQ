using System.Management.Automation;
using PSSharp.PowerShell.RabbitMQ.Commands.Management;

namespace PSSharp.PowerShell.RabbitMQ.Commands;

[Cmdlet(
    VerbsCommon.Get,
    Nouns.RabbitMqConnection,
    DefaultParameterSetName = VhostParameterSet,
    RemotingCapability = RemotingCapability.OwnedByCommand
)]
[OutputType(
    typeof(ConnectionDetails),
    ParameterSetName = [VhostParameterSet, AllVhostsParameterSet]
)]
[OutputType(typeof(UserConnectionDetails), ParameterSetName = [UserParameterSet])]
public sealed class GetRabbitMqConnectionCommand : RmqCmdlet
{
    private const string VhostParameterSet = "VHost";
    private const string AllVhostsParameterSet = "AllVirtualHosts";
    private const string UserParameterSet = "User";

    [Parameter(ValueFromPipelineByPropertyName = true, ValueFromPipeline = true, Position = 0)]
    [ArgumentCompletions("'127.0.0.1:5459 -> 127.0.0.1:5672'")]
    [SupportsWildcards]
    public string[] Name { get; set; } = [];

    [Parameter(
        ValueFromPipelineByPropertyName = true,
        ParameterSetName = VhostParameterSet,
        Position = 1
    )]
    [Parameter(
        ValueFromPipelineByPropertyName = true,
        ParameterSetName = AllVhostsParameterSet,
        Position = 1
    )]
    [Parameter(
        ValueFromPipelineByPropertyName = true,
        ParameterSetName = UserParameterSet,
        Mandatory = true,
        Position = 1
    )]
    [ArgumentCompletions("guest")]
    [SupportsWildcards]
    public string? UserName { get; set; }

    [Parameter(Mandatory = true)]
    [ValidateAbsoluteUri]
    [ValidateUriScheme("http", "https")]
    [ManagementServerUriCompleter]
    public Uri? ManagementUri { get; set; }

    [Parameter]
    [Credential]
    [ArgumentCompletions("guest")]
    public PSCredential? Credential { get; set; }

    [Parameter(ValueFromPipelineByPropertyName = true, ParameterSetName = VhostParameterSet)]
    [Alias("VirtualHost")]
    [ValidateNotNullOrEmpty]
    [VirtualHostCompleter]
    public string? VHost { get; set; } = "/";

    [Parameter(ParameterSetName = AllVhostsParameterSet)]
    [Alias("AllVHosts")]
    public SwitchParameter AllVirtualHosts
    {
        get => VHost is null;
        set => VHost = value ? null : VHost ?? "/";
    }

    /// <summary>
    /// Use the <c>/api/connections/username</c> endpoint which returns less data but may have different
    /// permissions applied. Use of this parameter makes the <see cref="UserName"/> parameter mandatory,
    /// and not support wildcards.
    /// </summary>
    [Parameter(DontShow = true, ParameterSetName = UserParameterSet)]
    public SwitchParameter UseUserEndpoint { get; set; }

    protected override async Task ProcessRecordAsync()
    {
        Assert.MandatoryParameter(ManagementUri, "all");

        var cred = Credential?.GetNetworkCredential() ?? ManagementUri.GetNetworkCredential();
        if (cred is null)
        {
            await this.ManagementCredentialRequiredError(ManagementUri).ConfigureAwait(false);
            return;
        }

        using var client = new ManagementClient(ManagementUri.WithoutCredential(), cred);

        if (UseUserEndpoint)
        {
            // Name and VHost become client-side filters
            await SearchByUser(client).ConfigureAwait(false);
        }
        else if (Name.Length > 0 && !Name.Any(WildcardPattern.ContainsWildcardCharacters))
        {
            // UserName and vhost become client-side filters
            await SearchByName(client).ConfigureAwait(false);
        }
        else if (VHost is not null)
        {
            // UserName and Name become client-side filters
            await SearchByVHost(client, VHost).ConfigureAwait(false);
        }
        else
        {
            // All fields are filtered client-side.
            await SearchAll(client).ConfigureAwait(false);
        }
    }

    private async Task SearchByName(ManagementClient client)
    {
        await InvokeAsync(() => WriteDebug("Searching by connection name."));

        foreach (var name in Name)
        {
            var connection = await client
                .GetConnection(name, PipelineStopToken)
                .ConfigureAwait(false);

            if (connection is null)
            {
                var er = Assert.ItemNotFound("Connection", name, nameof(ConnectionDetails.Name));
                await WriteErrorAsync(er).ConfigureAwait(false);
            }
            else if (!IsUserNameMatch(connection))
            {
                var er = Assert.ItemNotFound(
                    "Connection",
                    name,
                    nameof(ConnectionDetails.Name),
                    " with the specified user name"
                );
                await WriteErrorAsync(er).ConfigureAwait(false);
            }
            else if (!IsVHostMatch(connection))
            {
                var er = Assert.ItemNotFound(
                    "Connection",
                    name,
                    nameof(ConnectionDetails.Name),
                    " with the specified virtual host"
                );
                await WriteErrorAsync(er).ConfigureAwait(false);
            }
            else
            {
                await WriteObjectAsync(connection).ConfigureAwait(false);
            }
        }
    }

    private async Task SearchByUser(ManagementClient client)
    {
        await InvokeAsync(() => WriteDebug("Searching by user name."));
        Assert.MandatoryParameter(UserName, UserParameterSet);

        var found = false;
        var namesToFind = new HashSet<string>(
            Name.Where(n => !WildcardPattern.ContainsWildcardCharacters(n)),
            StringComparer.OrdinalIgnoreCase
        );

        await foreach (
            var connection in client
                .ListConnectionsByUser(UserName, PipelineStopToken)
                .ConfigureAwait(false)
        )
        {
            // VHost and AllVirtualHosts parameters are part of exclusive sets, so do not apply vhost filtering
            if (!IsNameMatch(connection, namesToFind))
            {
                continue;
            }

            found = true;
            await WriteObjectAsync(connection);
        }

        await WriteErrorForEachNameNotFound(namesToFind).ConfigureAwait(false);

        if (!found && namesToFind.Count == 0)
        {
            var er = Assert.ItemNotFound("Connection", UserName);
            await WriteErrorAsync(er).ConfigureAwait(false);
        }
    }

    private async Task SearchByVHost(ManagementClient client, string vhost)
    {
        await InvokeAsync(() => WriteDebug("Searching by virtual host."));

        var found = false;
        var namesToFind = new HashSet<string>(
            Name.Where(n => !WildcardPattern.ContainsWildcardCharacters(n)),
            StringComparer.OrdinalIgnoreCase
        );

        await foreach (
            var connection in client
                .ListConnectionsByVhost(vhost, PipelineStopToken)
                .ConfigureAwait(false)
        )
        {
            if (!IsUserNameMatch(connection))
            {
                continue;
            }
            if (!IsNameMatch(connection, namesToFind))
            {
                continue;
            }

            found = true;
            await WriteObjectAsync(connection);
        }

        await WriteErrorForEachNameNotFound(namesToFind).ConfigureAwait(false);

        if (
            !found
            && namesToFind.Count == 0
            && MyInvocation.BoundParameters.ContainsKey(nameof(VHost))
        )
        {
            var er = Assert.ItemNotFound(
                "Connection",
                VHost,
                suffix: UserName is null ? null : " matching the provided criteria"
            );
            await WriteErrorAsync(er).ConfigureAwait(false);
        }
    }

    private async Task SearchAll(ManagementClient client)
    {
        await InvokeAsync(() => WriteDebug("Searching with client side filters."));

        var namesToFind = new HashSet<string>(
            Name.Where(n => !WildcardPattern.ContainsWildcardCharacters(n)),
            StringComparer.OrdinalIgnoreCase
        );

        await foreach (
            var connection in client.ListConnections(PipelineStopToken).ConfigureAwait(false)
        )
        {
            if (!IsUserNameMatch(connection))
            {
                await InvokeAsync(() => WriteDebug("Connection does not match user name."));
                continue;
            }
            if (!IsVHostMatch(connection))
            {
                await InvokeAsync(() => WriteDebug("Connection does not match vhost."));
                continue;
            }
            if (!IsNameMatch(connection, namesToFind))
            {
                await InvokeAsync(() => WriteDebug("Connection does not match name."));
                continue;
            }

            await WriteObjectAsync(connection).ConfigureAwait(false);
        }

        await WriteErrorForEachNameNotFound(namesToFind).ConfigureAwait(false);
    }

    private bool IsNameMatch(UserConnectionDetails connection, HashSet<string> namesToFind)
    {
        if (Name.Length == 0 || namesToFind.Remove(connection.Name))
        {
            return true;
        }

        return Name.Where(WildcardPattern.ContainsWildcardCharacters)
            .Select(n => WildcardPattern.Get(n, WildcardOptions.IgnoreCase))
            .Any(wc => wc.IsMatch(connection.Name));
    }

    private bool IsUserNameMatch(UserConnectionDetails connection)
    {
        if (UserName is null)
        {
            return true;
        }
        else if (!WildcardPattern.ContainsWildcardCharacters(UserName))
        {
            return UserName.Equals(connection.User, StringComparison.OrdinalIgnoreCase);
        }
        else
        {
            return WildcardPattern
                .Get(UserName, WildcardOptions.IgnoreCase)
                .IsMatch(connection.User);
        }
    }

    private bool IsVHostMatch(UserConnectionDetails connection)
    {
        if (VHost is null)
        {
            return true;
        }
        return WildcardPattern.Get(VHost, WildcardOptions.IgnoreCase).IsMatch(connection.VHost);
    }

    private async Task WriteErrorForEachNameNotFound(HashSet<string> namesToFind)
    {
        foreach (var name in namesToFind)
        {
            var er = Assert.ItemNotFound(
                "Connection",
                name,
                nameof(ConnectionDetails.Name),
                suffix: " matching the provided criteria"
            );
            await WriteErrorAsync(er).ConfigureAwait(false);
        }
    }
}
