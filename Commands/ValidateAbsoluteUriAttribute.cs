using System.Management.Automation;

namespace PSSharp.PowerShell.RabbitMQ.Commands;

public sealed class ValidateAbsoluteUriAttribute : ValidateArgumentsAttribute
{
    protected override void Validate(object arguments, EngineIntrinsics engineIntrinsics)
    {
        if (arguments is not Uri uri)
        {
            return;
        }

        if (!uri.IsAbsoluteUri)
        {
            throw new ValidationMetadataException(
                "The RabbitMQ URI must be an absolute URI, not a relative URI."
            );
        }
    }
}
