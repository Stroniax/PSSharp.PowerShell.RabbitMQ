using System.Management.Automation;

namespace PSSharp.PowerShell.RabbitMQ;

/// <summary>
/// Describes that a parameter ought to be a <see cref="PSRabbitConnection"/>. This attribute may provide
/// transformation from a given name or instance id into the corresponding <see cref="PSRabbitConnection"/>
/// via <see cref="RabbitConnectionStore"/>.
/// </summary>
public sealed class RabbitConnectionAttribute : ArgumentTransformationAttribute
{
    public override object? Transform(EngineIntrinsics engineIntrinsics, object inputData)
    {
        var store = new RabbitConnectionStore(engineIntrinsics.SessionState);

        return inputData switch
        {
            string name => ConvertFromName(store, name),
            Guid instanceId => ConvertFromInstanceId(store, instanceId),
            _ => inputData,
        };
    }

    private static object? ConvertFromInstanceId(RabbitConnectionStore store, Guid instanceId)
    {
        var match = store.GetConnections().Where(c => c.InstanceId == instanceId).ToList();

        return match.Count == 1 ? match[0] : instanceId;
    }

    private static object? ConvertFromName(
        RabbitConnectionStore store,
        string name)
    {
        var match = store.GetConnections().Where(c => c.Name.Equals(name, StringComparison.OrdinalIgnoreCase)).ToList();

        return match.Count == 1
                ? match[0]
                : name;
    }


}
