using System.Collections;
using System.Diagnostics.CodeAnalysis;
using System.Management.Automation;
using System.Management.Automation.Language;

namespace PSSharp.PowerShell.RabbitMQ.Commands;

public sealed class QueueNameCompleterAttribute
    : ArgumentCompleterFactoryAttribute,
        IArgumentCompleter
{
    public IEnumerable<CompletionResult> CompleteArgument(
        string commandName,
        string parameterName,
        string wordToComplete,
        CommandAst commandAst,
        IDictionary fakeBoundParameters
    )
    {
        var quote = ArgumentCompletionQuote.Extract(wordToComplete, out wordToComplete);
        var toComplete = WildcardPattern.Get(wordToComplete + "*", WildcardOptions.IgnoreCase);

        var hasHost = TryGetHostName(fakeBoundParameters, out var hostName);
        var hasVhost = TryGetVirtualHost(fakeBoundParameters, out var vhost);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));

        return RmqManagementServer
            .All()
            .Where(s =>
                !hasHost || s.Uri.Host.Contains(hostName!, StringComparison.OrdinalIgnoreCase)
            )
            .Select(s => s.Client)
            .ToAsyncEnumerable()
            .SelectMany(client => client.ListQueues(hasVhost ? vhost : null, cts.Token))
            .Select(q => q.Name)
            .Where(q => toComplete.IsMatch(q))
            .Select(s => new CompletionResult(
                quote.Wrap(s),
                s,
                CompletionResultType.ParameterValue,
                s
            ))
            .ToBlockingEnumerable(cts.Token);
    }

    private static bool TryGetHostName(
        IDictionary fakeBoundParameters,
        [MaybeNullWhen(false)] out string hostName
    )
    {
        if (
            fakeBoundParameters.Contains("ManagementUri")
            && fakeBoundParameters["ManagementUri"] is Uri managementUri
        )
        {
            hostName = managementUri.Host;
            return true;
        }
        else if (
            fakeBoundParameters.Contains("Connection")
            && fakeBoundParameters["Connection"] is PSRabbitConnection conn
        )
        {
            hostName = conn.GetConnection().Endpoint.HostName;
            return true;
        }
        else
        {
            hostName = null;
            return false;
        }
    }

    private static bool TryGetVirtualHost(
        IDictionary fakeBoundParameters,
        [MaybeNullWhen(false)] out string vhost
    )
    {
        if (
            fakeBoundParameters.Contains("VHost") && fakeBoundParameters["VHost"] is string vhostArg
        )
        {
            vhost = vhostArg;
            return true;
        }
        else
        {
            vhost = null;
            return false;
        }
    }

    public override IArgumentCompleter Create()
    {
        return this;
    }
}
