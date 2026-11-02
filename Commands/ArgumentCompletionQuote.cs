using System.Text.RegularExpressions;

namespace PSSharp.PowerShell.RabbitMQ.Commands;

public readonly partial struct ArgumentCompletionQuote
{
    private readonly char? _quote;

    private ArgumentCompletionQuote(char? quote)
    {
        _quote = quote;
    }

    public static ArgumentCompletionQuote Extract(
        string wordToComplete,
        out string strippedWordToComplete
    )
    {
        ArgumentCompletionQuote quote;
        if (wordToComplete.StartsWith('\''))
        {
            strippedWordToComplete = wordToComplete.Substring(1);
            quote = new('\'');
        }
        else if (wordToComplete.StartsWith('"'))
        {
            strippedWordToComplete = wordToComplete.Substring(1);
            quote = new('"');
        }
        else
        {
            strippedWordToComplete = wordToComplete;
            quote = new();
        }

        if (quote._quote is not null && strippedWordToComplete.EndsWith(quote._quote.Value))
        {
            strippedWordToComplete = strippedWordToComplete.Substring(
                0,
                strippedWordToComplete.Length - 1
            );
        }

        return quote;
    }

    public string Wrap(string completion)
    {
        if (_quote is '\'')
        {
            return "'" + completion.Replace("'", "''") + "'";
        }
        else if (_quote is '"')
        {
            return '"' + PSInterpolationSyntax().Replace(completion, "`$1") + '"';
        }
        else if (!PSBareString().IsMatch(completion))
        {
            return "'" + completion.Replace("'", "''") + "'";
        }
        else
        {
            return completion;
        }
    }

    [GeneratedRegex("^[A-Za-z0-9-_]+$")]
    private static partial Regex PSBareString();

    [GeneratedRegex("([\"@$`])")]
    private static partial Regex PSInterpolationSyntax();
}
