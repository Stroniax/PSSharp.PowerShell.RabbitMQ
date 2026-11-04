using System.Collections.Immutable;
using System.Diagnostics;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace PSSharp.PowerShell.RabbitMQ.Commands.Management;

/// <summary>
/// Convert a json object to a C# value using the most standard readonly primitives:
/// - ImmutableDictionary for IDictionary and JSON object
/// - ImmutableArray for IReadOnlyList and JSON array
/// - Int32 or Double for number
/// - String
/// - Boolean
/// - Null
/// </summary>
internal sealed class PrimitiveObjectJsonConverter : JsonConverter<IDictionary<string, object?>>
{
    public override IDictionary<string, object?>? Read(
        ref Utf8JsonReader reader,
        Type typeToConvert,
        JsonSerializerOptions options
    )
    {
        var result = ImmutableDictionary.CreateBuilder<string, object?>(
            StringComparer.OrdinalIgnoreCase
        );

        while (reader.Read())
        {
            if (reader.TokenType == JsonTokenType.EndObject)
            {
                break;
            }

            var key = reader.GetString()!;

            reader.Read();
            object? value = JsonPrimitiveReader.ReadPrimitive(ref reader, options);

            result[key] = value;
        }

        return result.ToImmutable();
    }

    public override void Write(
        Utf8JsonWriter writer,
        IDictionary<string, object?> value,
        JsonSerializerOptions options
    )
    {
        writer.WriteStartObject();
        foreach (var (key, prop) in value)
        {
            writer.WritePropertyName(key);
            JsonSerializer.Serialize(writer, prop, options);
        }
        writer.WriteEndObject();
    }
}
