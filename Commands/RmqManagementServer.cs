using System.Collections.Concurrent;
using PSSharp.PowerShell.RabbitMQ.Commands.Management;

namespace PSSharp.PowerShell.RabbitMQ.Commands;

public sealed class RmqManagementServer : IDisposable
{
    private bool _isDisposed;
    public Uri Uri { get; }
    internal ManagementClient Client
    {
        get
        {
            ObjectDisposedException.ThrowIf(_isDisposed, this);
            return field;
        }
    }

    internal RmqManagementServer(Uri uri, ManagementClient client)
    {
        Uri = uri;
        Client = client;
    }

    private static readonly ConcurrentDictionary<Uri, RmqManagementServer> _servers = new();

    internal static IEnumerable<Uri> KnownUris() => _servers.Keys;

    internal static void AddOrUpdate(RmqManagementServer data)
    {
        _servers.AddOrUpdate(data.Uri, data, (_, _) => data);
    }

    internal static bool Remove(Uri uri)
    {
        if (_servers.TryRemove(uri, out var server))
        {
            server.Dispose();
            return true;
        }
        else
        {
            return false;
        }
    }

    internal static RmqManagementServer[] All() => _servers.Values.ToArray();

    public void Dispose()
    {
        if (_isDisposed)
        {
            return;
        }
        _isDisposed = true;
        Client.Dispose();
        Remove(Uri);
    }
}
