using System.Collections;
using System.Management.Automation;
using System.Management.Automation.Language;
using System.Management.Automation.Runspaces;

namespace PSSharp.PowerShell.RabbitMQ.Commands;

public sealed class RabbitConnectionCompleterAttribute : ArgumentCompleterFactoryAttribute, IArgumentCompleter
{
    public override IArgumentCompleter Create() => this;

    public IEnumerable<CompletionResult> CompleteArgument(string commandName, string parameterName, string wordToComplete, CommandAst commandAst, IDictionary fakeBoundParameters)
    {
        var store = RabbitConnectionStore.Current;
        var quote = string.Empty;
        var toComplete = wordToComplete;

        if (toComplete.StartsWith('\'') || toComplete.StartsWith('"'))
        {
            quote = toComplete[..1];
            toComplete = toComplete[1..];
        }

        if (quote.Length > 0 && toComplete.EndsWith(quote))
        {
            toComplete = toComplete[..^1];
        }

        var pattern = toComplete + "*";

        return parameterName switch
        {
            "InstanceId" => store.GetConnections()
                    .Where(c => WildcardPattern.Get(pattern, WildcardOptions.IgnoreCase).IsMatch(c.Name))
                    .Select(c => new CompletionResult(
                        quote + c.InstanceId + quote,
                        c.InstanceId.ToString(),
                        CompletionResultType.ParameterValue,
                        $"{c.InstanceId} \"{c.Name}\"")),
            _ => store.GetConnections()
                                .Where(c => WildcardPattern.Get(pattern, WildcardOptions.IgnoreCase).IsMatch(c.Name))
                                .Select(c =>
                                {
                                    var requireQuote = c.Name.Any(ch => !char.IsAsciiLetterOrDigit(ch));
                                    var outputQuote = quote.Length > 0
                                        ? quote
                                        : requireQuote ? "'" : string.Empty;

                                    return new CompletionResult(
                                        outputQuote + c.Name + outputQuote,
                                        c.Name,
                                        CompletionResultType.ParameterValue,
                                        c.Name);
                                }),
        };
    }

}
