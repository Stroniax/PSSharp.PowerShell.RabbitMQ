using System.Management.Automation;

namespace PSSharp.PowerShell.RabbitMQ.Commands;

public sealed class VirtualHostCompleterAttribute : RabbitMqCompletionAttribute
{
    protected override IAsyncEnumerable<ArgumentCompletionText> CompleteArgument(
        string parameterName,
        WildcardPattern patternToComplete,
        string? hostName,
        string? vhost,
        CancellationToken cancellationToken
    )
    {
        return RmqManagementServer
            .All()
            .Where(s =>
                hostName is null
                || s.Uri.Host.Contains(hostName, StringComparison.OrdinalIgnoreCase)
            )
            .Select(s => s.Client)
            .ToAsyncEnumerable()
            .SelectMany(client => client.ListVirtualHosts(cancellationToken))
            .Select(v => v.Name)
            .Where(v => patternToComplete.IsMatch(v))
            .Select(v => new ArgumentCompletionText(v))
            .DefaultIfEmpty(new("/"));
    }
}
