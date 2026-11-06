using System.Management.Automation;
using PSSharp.PowerShell.RabbitMQ.Commands.Management;

namespace PSSharp.PowerShell.RabbitMQ.Commands;

[Cmdlet(
    VerbsCommon.Remove,
    Nouns.RabbitMqConnection,
    SupportsShouldProcess = true,
    RemotingCapability = RemotingCapability.OwnedByCommand
)]
public sealed class RemoveRabbitMqConnectionCommand : RmqCmdlet
{
    [Parameter(
        Mandatory = true,
        Position = 0,
        ValueFromPipelineByPropertyName = true,
        ValueFromPipeline = true
    )]
    [Alias("ConnectionName", "Connection")]
    [ArgumentCompletions("'127.0.0.1:5459 -> 127.0.0.1:5672'")]
    public string Name { get; set; } = string.Empty;

    [Parameter(Position = 1, ValueFromPipelineByPropertyName = true)]
    [ValidateNotNullOrWhiteSpace]
    [ArgumentCompletions("'Shut down by PowerShell script'")]
    public string? Reason { get; set; }

    [Parameter(Mandatory = true)]
    [ValidateAbsoluteUri]
    [ValidateUriScheme("http", "https")]
    [ManagementServerUriCompleter]
    public Uri? ManagementUri { get; set; }

    [Parameter]
    [Credential]
    [ArgumentCompletions("guest")]
    public PSCredential? Credential { get; set; }

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

        var (shouldProcess, reason) = await ShouldProcess();
        if (!shouldProcess)
        {
            await WhatIfConnectionExists(client, reason).ConfigureAwait(false);
            return;
        }

        var deleted = await client
            .DeleteConnection(Name, Reason, PipelineStopToken)
            .ConfigureAwait(false);
        if (!deleted)
        {
            var er = Assert.ItemNotFound("Connection", Name);
            await WriteErrorAsync(er).ConfigureAwait(false);
        }
    }

    private async Task<(bool, ShouldProcessReason)> ShouldProcess()
    {
        var reason = ShouldProcessReason.None;
        var shouldProcess = await InvokeAsync(() =>
            {
                return ShouldProcess(
                    $"Removing RabbitMQ client connection: \"{Name}\".",
                    $"Remove RabbitMQ client connection: \"{Name}\"?",
                    "Remove Connection",
                    out reason
                );
            })
            .ConfigureAwait(false);
        return (shouldProcess, reason);
    }

    private async Task WhatIfConnectionExists(ManagementClient client, ShouldProcessReason reason)
    {
        if (reason == ShouldProcessReason.WhatIf)
        {
            // check if the connection exists, otherwise report error
            var connection = await client
                .GetConnection(Name, PipelineStopToken)
                .ConfigureAwait(false);
            if (connection is null)
            {
                var er = Assert.ItemNotFound("Connection", Name);
                await WriteErrorAsync(er).ConfigureAwait(false);
            }
        }
    }
}
