using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Management.Automation;
using System.Runtime.CompilerServices;

namespace PSSharp.PowerShell.RabbitMQ.Commands;

internal static class Assert
{
    public static void MandatoryParameter<T>(
        [NotNull] T? parameter,
        string parameterSetName,
        [CallerArgumentExpression(nameof(parameter))] string? parameterName = null
    )
    {
        if (parameter is not null)
        {
            return;
        }

        throw new UnreachableException(
            $"The parameter \"{parameterName}\" is mandatory for parameter set \"{parameterSetName}\". "
                + $"The value of the parameter is null."
        );
    }

    [DoesNotReturn]
    public static void ParameterSetExhausted(string parameterSetName)
    {
        throw new UnreachableException(
            $"The parameter set \"{parameterSetName}\" has no implementation. This parameter set should "
                + $"not have been called. This is a module error. Please contact the module author."
        );
    }

    /// <summary>
    /// Produce a <see cref="ErrorRecord"/> containing an <see cref="ItemNotFoundException"/>
    /// for consistent use accross the module.
    /// </summary>
    /// <typeparam name="T">Type of argument to potentially alleviate boxing.</typeparam>
    /// <param name="itemType">Type of resource of which the user wanted and could not be found.</param>
    /// <param name="key">Value that was compared to try to find the resource.</param>
    /// <param name="keyName">Name of the argument, parameter, or field that was compared against.</param>
    /// <param name="suffix">Trailing phrase injected between the end of the message and the final period.</param>
    /// <returns></returns>
    public static ErrorRecord ItemNotFound<T>(
        string itemType,
        T key,
        [CallerArgumentExpression(nameof(key))] string? keyName = null,
        string? suffix = null,
        Exception? innerException = null
    )
    {
        var ex = new ItemNotFoundException($"The {itemType} does not exist.", innerException);

        return new ErrorRecord(ex, $"{keyName}NotFound", ErrorCategory.ObjectNotFound, key)
        {
            ErrorDetails = new(
                $"The {itemType} does not exist with \"{keyName}\" = \"{key}\"{suffix}."
            ),
        };
    }
}
