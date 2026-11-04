using System.Diagnostics;
using System.Text.Json;

namespace PSSharp.PowerShell.RabbitMQ.Commands.Management;

internal static class JsonPrimitiveReader
{
    public static object? ReadPrimitive(ref Utf8JsonReader reader, JsonSerializerOptions? options)
    {
        return reader.TokenType switch
        {
            JsonTokenType.Number => reader.TryGetInt32(out var i32) ? i32 : reader.GetDouble(),
            JsonTokenType.String => reader.GetString(),
            JsonTokenType.StartArray => JsonSerializer.Deserialize<IReadOnlyList<object?>>(
                ref reader,
                options
            ),
            JsonTokenType.Null => null,
            JsonTokenType.False => false,
            JsonTokenType.True => true,
            JsonTokenType.StartObject => JsonSerializer.Deserialize<
                IReadOnlyDictionary<string, object?>
            >(ref reader, options),
            _ => throw new UnreachableException($"Unhandled json token type {reader.TokenType}."),
        };
    }
}
