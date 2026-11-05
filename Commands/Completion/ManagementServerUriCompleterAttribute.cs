using System.Collections;
using System.Management.Automation;
using System.Management.Automation.Language;

namespace PSSharp.PowerShell.RabbitMQ.Commands;

public sealed class ManagementServerUriCompleterAttribute
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

        var management = RmqManagementServer.KnownUris().Select(u => u.ToString().TrimEnd('/'));
        var client = RabbitConnectionStore
            .Current.GetConnections()
            .Select(c => c.GetConnection())
            .Select(c => $"amqp://{c.Endpoint.HostName}:{c.Endpoint.Port}");

        var uris = management.Concat(client).Append("http://localhost:15672");
        return uris.Where(u =>
                WildcardPattern
                    .Get(wordToComplete.Trim(['"', '\'']) + '*', WildcardOptions.IgnoreCase)
                    .IsMatch(u)
            )
            .Select(uri => new CompletionResult(
                quote.Wrap(uri),
                uri,
                CompletionResultType.ParameterValue,
                uri
            ));
    }

    public override IArgumentCompleter Create()
    {
        return this;
    }
}
