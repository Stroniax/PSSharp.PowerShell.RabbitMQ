using System.Management.Automation;

namespace PSSharp.PowerShell.RabbitMQ.Commands;

public sealed class ValidateUriSchemeAttribute(params string[] schemes) : ValidateArgumentsAttribute
{
    protected override void Validate(object arguments, EngineIntrinsics engineIntrinsics)
    {
        if (arguments is not Uri uri)
        {
            return;
        }

        if (!schemes.Contains(uri.Scheme, StringComparer.OrdinalIgnoreCase))
        {
            var acceptable = string.Join(", ", schemes.Select(s => $"\"{s}\""));
            throw new ValidationMetadataException(
                $"The RabbitMQ URI scheme must be one of {acceptable}."
            );
        }
    }
}
