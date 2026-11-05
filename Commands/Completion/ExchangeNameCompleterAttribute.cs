using System.Management.Automation;

namespace PSSharp.PowerShell.RabbitMQ.Commands;

public sealed class ExchangeNameCompleterAttribute : RabbitMqCompletionAttribute
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
                hostName is null || s.Uri.Host.Equals(hostName, StringComparison.OrdinalIgnoreCase)
            )
            .Select(s => s.Client)
            .ToAsyncEnumerable()
            .SelectMany(client => client.ListExchanges(vhost, cancellationToken))
            .Select(e => e.Name)
            .DefaultIfEmpty("amq.default")
            .Where(e => patternToComplete.IsMatch(e))
            .Select(c => new ArgumentCompletionText(
                c is not { Length: > 1 } ? "amq.default" : c,
                c is not { Length: > 1 } ? "amq.default (alias: empty string '')" : null
            ));
    }
}
