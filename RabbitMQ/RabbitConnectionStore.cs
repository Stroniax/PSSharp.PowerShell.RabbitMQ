using System.Management.Automation;

namespace PSSharp.PowerShell.RabbitMQ;

/// <summary>
/// A wrapper over a specific variable in the <paramref name="variables"/> store that is used
/// to store RabbitMQ sessions.
/// </summary>
/// <param name="variables"></param>
public sealed class RabbitConnectionStore(SessionState ss)
{
    private PSVariableIntrinsics Variables => ss.PSVariable;

    private const string VariableName = "__RabbitMqSessions";

    public IEnumerable<PSRabbitConnection> GetConnections()
    {
        return GetStore().AsEnumerable();
    }

    public void Add(PSRabbitConnection session)
    {
        GetStore().Add(session);
    }

    public bool Remove(PSRabbitConnection session)
    {
        return GetStore().Remove(session);
    }

    private List<PSRabbitConnection> GetStore()
    {
        var psvar = Variables.Get(VariableName);
        if (psvar is null)
        {
            psvar = new PSVariable(VariableName, new List<PSRabbitConnection>(), ScopedItemOptions.Private | ScopedItemOptions.ReadOnly | ScopedItemOptions.Constant);
            Variables.Set(psvar);
        }

        return (List<PSRabbitConnection>)psvar.Value;
    }

    public override string ToString()
    {
        var sessions = GetConnections();
        return $"RabbitMQ Session Store: {sessions.Count()} session(s)";
    }
}