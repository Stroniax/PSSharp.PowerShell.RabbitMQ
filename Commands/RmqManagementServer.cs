using System.Collections.Concurrent;

namespace PSSharp.PowerShell.RabbitMQ.Commands;

public sealed class RmqManagementServer(Uri uri, string vhost)
{
    private static readonly ConcurrentDictionary<string, RmqManagementServer> _servers = new(
        StringComparer.OrdinalIgnoreCase
    );

    internal static IEnumerable<Uri> KnownUris() =>
        _servers.Keys.Select(k => new Uri(k.Split("+<vhost>+")[0]));

    internal static void AddOrUpdate(RmqManagementServer data)
    {
        _servers.AddOrUpdate($"{data.Uri}+<vhost>+{data.VHost}", data, (_, _) => data);
    }

    internal static bool Remove(Uri uri, string vhost)
    {
        return _servers.TryRemove($"{uri}+<vhost>{vhost}", out _);
    }

    internal static RmqManagementServer[] All() => _servers.Values.ToArray();

    public Uri Uri { get; } = uri;
    public string VHost { get; } = vhost;

    public List<string> Queues { get; set; } = [];
    public List<string> Exchanges { get; set; } = [];
}
