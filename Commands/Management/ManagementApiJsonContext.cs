using System.Text.Json;
using System.Text.Json.Serialization;

namespace PSSharp.PowerShell.RabbitMQ.Commands.Management;

[JsonSerializable(typeof(ApiError))]
[JsonSerializable(typeof(Message[]))]
[JsonSerializable(typeof(MessageRequest))]
[JsonSerializable(typeof(PublishRequest))]
[JsonSerializable(typeof(PublishResponse))]
[JsonSerializable(typeof(QueueDeclareOptions))]
[JsonSerializable(typeof(QueueDetails))]
[JsonSerializable(typeof(VirtualHost))]
[JsonSerializable(typeof(Dictionary<string, object?>))]
[JsonSerializable(typeof(IDictionary<string, object?>))]
[JsonSerializable(typeof(IReadOnlyDictionary<string, object?>))]
[JsonSerializable(typeof(IReadOnlyList<object?>))]
[JsonSerializable(typeof(JsonElement))]
[JsonSourceGenerationOptions(
    PropertyNamingPolicy = JsonKnownNamingPolicy.SnakeCaseLower,
    DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
    UnknownTypeHandling = JsonUnknownTypeHandling.JsonElement,
    Converters = [
        typeof(BasicPropertiesJsonConverter),
        typeof(PrimitiveArrayJsonConverter),
        typeof(PrimitiveObjectJsonConverter),
        typeof(QueueDetailsJsonConverter),
    ]
)]
public sealed partial class ManagementApiJsonContext : JsonSerializerContext;
