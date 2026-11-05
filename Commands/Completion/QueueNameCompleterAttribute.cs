using System.Management.Automation;

namespace PSSharp.PowerShell.RabbitMQ.Commands;

public sealed class QueueNameCompleterAttribute : RabbitMqCompletionAttribute
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
            .SelectMany(client => client.ListQueues(vhost, cancellationToken))
            .Select(q => q.Name)
            .Where(q => patternToComplete.IsMatch(q))
            .Select(c => new ArgumentCompletionText(c));
    }
}
