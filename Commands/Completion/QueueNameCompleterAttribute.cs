using System.Collections;
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

        return RmqManagementServer
            .All()
            .SelectMany(s => s.Queues)
            .Where(s => toComplete.IsMatch(s))
            .Select(s => new CompletionResult(
                quote.Wrap(s),
                s,
                CompletionResultType.ParameterValue,
                s
            ));
    }

    public override IArgumentCompleter Create()
    {
        return this;
    }
}
