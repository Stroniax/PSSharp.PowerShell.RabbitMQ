using System.Collections.Immutable;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace PSSharp.PowerShell.RabbitMQ.Commands.Management;

/// <summary>
/// Convert a array object to a C# value using the most standard readonly primitives:
/// - ImmutableDictionary for IDictionary and JSON object
/// - ImmutableArray for IReadOnlyList and JSON array
/// - Int32 or Double for number
/// - String
/// - Boolean
/// - Null
/// </summary>
internal sealed class PrimitiveArrayJsonConverter : JsonConverter<IReadOnlyList<object?>>
{
    public override IReadOnlyList<object?>? Read(
        ref Utf8JsonReader reader,
        Type typeToConvert,
        JsonSerializerOptions options
    )
    {
        var list = ImmutableArray.CreateBuilder<object?>();

        while (reader.Read())
        {
            if (reader.TokenType == JsonTokenType.EndArray)
            {
                break;
            }

            var value = JsonPrimitiveReader.ReadPrimitive(ref reader, options);
            list.Add(value);
        }

        return list.ToImmutableArray();
    }

    public override void Write(
        Utf8JsonWriter writer,
        IReadOnlyList<object?> value,
        JsonSerializerOptions options
    )
    {
        writer.WriteStartArray();

        foreach (var v in value)
        {
            JsonSerializer.Serialize(writer, v, options);
        }

        writer.WriteEndArray();
    }
}
