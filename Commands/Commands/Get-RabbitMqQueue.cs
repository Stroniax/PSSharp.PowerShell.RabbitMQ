using System.Diagnostics;
using System.Management.Automation;
using PSSharp.PowerShell.RabbitMQ.Commands.Management;

namespace PSSharp.PowerShell.RabbitMQ.Commands;

[Cmdlet(
    VerbsCommon.Get,
    Nouns.RabbitMqQueue,
    RemotingCapability = RemotingCapability.OwnedByCommand,
    DefaultParameterSetName = VhostParameterSet
)]
public sealed class GetRabbitMqQueueCommand : RmqCmdlet
{
    private const string AllVhostsParameterSet = "AllVirtualHosts";
    private const string VhostParameterSet = "VHost";

    [Parameter(ValueFromPipelineByPropertyName = true, Position = 0)]
    [Alias("QueueName", "Name")]
    [SupportsWildcards]
    [QueueNameCompleter]
    public string? Queue { get; set; }

    [Parameter(Mandatory = true)]
    [ManagementServerUriCompleter]
    [ValidateAbsoluteUri]
    [ValidateUriScheme("http", "https")]
    public Uri? ManagementUri { get; set; }

    [Parameter]
    [Credential]
    [ArgumentCompletions("guest")]
    public PSCredential? Credential { get; set; }

    [Parameter(
        ValueFromPipelineByPropertyName = true,
        ParameterSetName = VhostParameterSet,
        Position = 1
    )]
    [Alias("VirtualHost")]
    [ValidateNotNullOrEmpty]
    [VirtualHostCompleter]
    public string VHost { get; set; } = "/";

    [Parameter(ParameterSetName = AllVhostsParameterSet)]
    public SwitchParameter AllVirtualHosts { get; set; }

    protected override async Task ProcessRecordAsync()
    {
        Debug.Assert(
            ManagementUri is not null,
            "ManagementUri is a mandatory parameter and must not be null."
        );

        var cred = Credential?.GetNetworkCredential() ?? ManagementUri.GetNetworkCredential();

        if (cred is null)
        {
            await this.ManagementCredentialRequiredError(ManagementUri);
            return;
        }

        using var client = new ManagementClient(ManagementUri.WithoutCredential(), cred);
        var vhost = AllVirtualHosts ? null : VHost;

        if (
            vhost is not null
            && Queue is not null
            && !WildcardPattern.ContainsWildcardCharacters(Queue)
        )
        {
            try
            {
                var queue = await client.GetQueue(vhost, Queue, PipelineStopToken);
                await WriteObjectAsync(queue);
            }
            catch (HttpRequestException httpex)
                when (httpex.StatusCode == System.Net.HttpStatusCode.NotFound)
            {
                var ex = new ItemNotFoundException("The queue does not exist.", httpex);
                var er = new ErrorRecord(ex, "QueueNotFound", ErrorCategory.ObjectNotFound, Queue)
                {
                    ErrorDetails = new($"No queue named \"{Queue}\" exists on VHost \"{vhost}\"."),
                };
                await WriteErrorAsync(er);
            }
            return;
        }

        var wc =
            Queue is not null && WildcardPattern.ContainsWildcardCharacters(Queue)
                ? WildcardPattern.Get(Queue, WildcardOptions.IgnoreCase)
                : null;
        var found = false;

        await foreach (var queue in client.ListQueues(vhost, PipelineStopToken))
        {
            if (wc is not null && !wc.IsMatch(queue.Name))
            {
                continue;
            }
            else if (
                Queue is not null
                && !Queue.Equals(queue.Name, StringComparison.OrdinalIgnoreCase)
            )
            {
                continue;
            }

            found = true;
            await WriteObjectAsync(queue);
        }

        if (!found && wc is null && Queue is not null)
        {
            var ex = new ItemNotFoundException("The queue does not exist.");
            var er = new ErrorRecord(ex, "QueueNotFound", ErrorCategory.ObjectNotFound, Queue)
            {
                ErrorDetails = new(
                    $"No queue named \"{Queue}\" was found matching the provided criteria."
                ),
            };
            await WriteErrorAsync(er);
        }
    }
}
