using System.Management.Automation;
using System.Net;

namespace PSSharp.PowerShell.RabbitMQ.Commands.Management;

internal static class CredentialExtensions
{
    extension(Uri @this)
    {
        public NetworkCredential? GetNetworkCredential()
        {
            if (string.IsNullOrEmpty(@this.UserInfo))
            {
                return null;
            }

            var parts = @this.UserInfo.Split(':', 2);
            var user = parts[0];
            var pass = parts.Length == 2 ? parts[1] : string.Empty;

            return new NetworkCredential(
                Uri.UnescapeDataString(user),
                Uri.UnescapeDataString(pass)
            );
        }

        public Uri WithoutCredential()
        {
            return new UriBuilder(@this) { UserName = "", Password = "" }.Uri;
        }
    }

    extension(RmqCmdlet @this)
    {
        public async Task ManagementCredentialRequiredError(Uri managementUri)
        {
            var ex = new ArgumentException("A credential must be provided for the Management API.");
            var er = new ErrorRecord(
                ex,
                "CredentialRequired",
                ErrorCategory.AuthenticationError,
                managementUri
            );
            await @this.WriteErrorAsync(er).ConfigureAwait(false);
        }
    }
}
