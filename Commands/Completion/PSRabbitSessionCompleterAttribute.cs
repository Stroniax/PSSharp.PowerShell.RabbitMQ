using System.Collections;
using System.Management.Automation;
using System.Management.Automation.Language;

namespace PSSharp.PowerShell.RabbitMQ.Commands;

public sealed class PSRabbitSessionCompleterAttribute
    : ArgumentCompleterFactoryAttribute,
        IArgumentCompleter
{
    public override IArgumentCompleter Create() => this;

    public IEnumerable<CompletionResult> CompleteArgument(
        string commandName,
        string parameterName,
        string wordToComplete,
        CommandAst commandAst,
        IDictionary fakeBoundParameters
    )
    {
        var store = PSRabbitSessionStore.Current;
        var quote = ArgumentCompletionQuote.Extract(wordToComplete, out wordToComplete);
        var pattern = WildcardPattern.Get(wordToComplete + "*", WildcardOptions.IgnoreCase);

        return parameterName switch
        {
            "InstanceId" => store
                .GetSessions()
                .Where(c => pattern.IsMatch(c.Name))
                .Select(c => new CompletionResult(
                    quote.Wrap(c.InstanceId.ToString()),
                    c.InstanceId.ToString(),
                    CompletionResultType.ParameterValue,
                    $"{c.InstanceId} \"{c.Name}\""
                )),
            _ => store
                .GetSessions()
                .Where(c => pattern.IsMatch(c.Name))
                .Select(c => new CompletionResult(
                    quote.Wrap(c.Name),
                    c.Name,
                    CompletionResultType.ParameterValue,
                    c.Name
                )),
        };
    }
}
