using System.Management.Automation;

namespace PSSharp.PowerShell.RabbitMQ.Commands;

/// <summary>
/// Describes that a parameter ought to be a <see cref="PSRabbitSession"/>. This attribute may provide
/// transformation from a given name or instance id into the corresponding <see cref="PSRabbitSession"/>
/// via <see cref="PSRabbitSessionStore"/>.
/// </summary>
public sealed class PSRabbitSessionAttribute : ArgumentTransformationAttribute
{
    public override object? Transform(EngineIntrinsics engineIntrinsics, object inputData)
    {
        var store = PSRabbitSessionStore.Current;

        return inputData switch
        {
            string name => ConvertFromName(store, name),
            Guid instanceId => ConvertFromInstanceId(store, instanceId),
            _ => inputData,
        };
    }

    private static object? ConvertFromInstanceId(PSRabbitSessionStore store, Guid instanceId)
    {
        var match = store.GetSessions().Where(c => c.InstanceId == instanceId).ToList();

        return match.Count == 1 ? match[0] : instanceId;
    }

    private static object? ConvertFromName(PSRabbitSessionStore store, string name)
    {
        var match = store
            .GetSessions()
            .Where(c => c.Name.Equals(name, StringComparison.OrdinalIgnoreCase))
            .ToList();

        return match.Count == 1 ? match[0] : name;
    }
}
