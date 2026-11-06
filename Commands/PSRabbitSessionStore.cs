using System.Collections.Concurrent;
using System.Management.Automation.Runspaces;
using System.Runtime.CompilerServices;

namespace PSSharp.PowerShell.RabbitMQ.Commands;

/// <summary>
/// A wrapper over a specific collection of <see cref="PSRabbitSession"/>s associated with
/// a <see cref="Runspace"/>.
/// </summary>
/// <param name="variables"></param>
public sealed class PSRabbitSessionStore
{
    private static readonly ConditionalWeakTable<Runspace, PSRabbitSessionStore> _stores = [];

    private readonly ConcurrentDictionary<Guid, PSRabbitSession> _store = [];

    /// <summary>
    /// Todo: use <c>AsyncLocal</c> in some way so that this works across async contexts.
    /// </summary>
    internal static PSRabbitSessionStore Current =>
        _stores.GetOrAdd(
            Runspace.DefaultRunspace ?? throw new Exception("No default runspace"),
            static _ => new()
        );

    public IEnumerable<PSRabbitSession> GetSessions()
    {
        return _store.Values.AsEnumerable();
    }

    public void Add(PSRabbitSession session)
    {
        if (HasSessionWithName(session.Name))
        {
            throw new InvalidOperationException(
                "A session with the provided name already exists in the store."
            );
        }

        if (!_store.TryAdd(session.InstanceId, session))
        {
            throw new InvalidOperationException(
                "A session with the provided InstanceId already exists in the store."
            );
        }
    }

    public bool Remove(PSRabbitSession session)
    {
        return _store.TryRemove(session.InstanceId, out _);
    }

    /// <summary>
    /// Checks if any session with the provided name already exists. The store does not permit
    /// duplicate session names.
    /// </summary>
    /// <param name="name"></param>
    /// <returns></returns>
    public bool HasSessionWithName(string name) =>
        _store.Values.Any(s => s.Name.Equals(name, StringComparison.OrdinalIgnoreCase));

    public override string ToString()
    {
        return $"RabbitMQ Session Store: {_store.Count} session(s)";
    }
}
