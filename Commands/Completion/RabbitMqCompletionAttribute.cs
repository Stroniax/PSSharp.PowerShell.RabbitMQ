using System.Collections;
using System.Management.Automation;
using System.Management.Automation.Language;

namespace PSSharp.PowerShell.RabbitMQ.Commands;

public abstract class RabbitMqCompletionAttribute
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

        var hostName = TryGetHostName(fakeBoundParameters);
        var vhost = TryGetVirtualHost(fakeBoundParameters);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));

        return CompleteArgument(parameterName, toComplete, hostName, vhost, cts.Token)
            .Select(c => c.ToCompletionResult(quote))
            .ToBlockingEnumerable(cts.Token);
    }

    public sealed override IArgumentCompleter Create()
    {
        return this;
    }

    protected abstract IAsyncEnumerable<ArgumentCompletionText> CompleteArgument(
        string parameterName,
        WildcardPattern patternToComplete,
        string? hostName,
        string? vhost,
        CancellationToken cancellationToken
    );

    private static string? TryGetHostName(IDictionary fakeBoundParameters)
    {
        if (
            fakeBoundParameters.Contains("ManagementUri")
            && fakeBoundParameters["ManagementUri"] is Uri managementUri
        )
        {
            return managementUri.Host;
        }
        else if (
            fakeBoundParameters.Contains("ManagementUri")
            && fakeBoundParameters["ManagementUri"] is string rawManagementUri
            && Uri.TryCreate(rawManagementUri, UriKind.Absolute, out var parsedManagementUri)
        )
        {
            return parsedManagementUri.Host;
        }
        else if (
            fakeBoundParameters.Contains("Connection")
            && fakeBoundParameters["Connection"] is PSRabbitConnection conn
        )
        {
            return conn.GetConnection().Endpoint.HostName;
        }
        else if (
            fakeBoundParameters.Contains("Connection")
            && fakeBoundParameters["Connection"] is string connectionName
        )
        {
            return RabbitConnectionStore
                .Current.GetConnections()
                .FirstOrDefault(c =>
                    c.Name.Equals(connectionName, StringComparison.OrdinalIgnoreCase)
                )
                ?.GetConnection()
                .Endpoint.HostName;
        }
        else
        {
            return null;
        }
    }

    private static string? TryGetVirtualHost(IDictionary fakeBoundParameters)
    {
        if (
            fakeBoundParameters.Contains("VHost") && fakeBoundParameters["VHost"] is string vhostArg
        )
        {
            return vhostArg;
        }
        else
        {
            // While we could supply the default vhost if the ManagementUri is specified without the
            // vhost parameter, I think it's better to return null so that all vhosts can be listed
            // for completion. This allows the user to add the vhost parameter next and still get the
            // completion they are looking for
            return null;
        }
    }
}
