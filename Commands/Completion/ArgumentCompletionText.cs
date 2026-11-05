using System.Management.Automation;

namespace PSSharp.PowerShell.RabbitMQ.Commands;

public readonly record struct ArgumentCompletionText
{
    public ArgumentCompletionText(string content)
    {
        Content = content;
    }

    public ArgumentCompletionText(string content, string tooltip)
        : this(content)
    {
        Tooltip = tooltip;
    }

    public readonly string Content;
    public readonly string? Tooltip;

    public static implicit operator ArgumentCompletionText(string content)
    {
        return new(content);
    }

    public CompletionResult ToCompletionResult(ArgumentCompletionQuote quote)
    {
        return new(
            quote.Wrap(Content),
            Content,
            CompletionResultType.ParameterValue,
            Tooltip ?? Content
        );
    }
}
