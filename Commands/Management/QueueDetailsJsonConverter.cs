using System.Collections.Immutable;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace PSSharp.PowerShell.RabbitMQ.Commands.Management;

internal sealed class QueueDetailsJsonConverter : JsonConverter<QueueDetails>
{
    public override QueueDetails? Read(
        ref Utf8JsonReader reader,
        Type typeToConvert,
        JsonSerializerOptions options
    )
    {
        string? name = null;
        uint messages = 0;
        DateTimeOffset? idleSince = null;
        bool autoDelete = false;
        int consumers = 0;
        var props = ImmutableDictionary.CreateBuilder<string, object?>(
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
            switch (key)
            {
                case "name":
                    name = reader.GetString();
                    break;
                case "messages":
                    messages = reader.GetUInt32();
                    break;
                case "idle_since":
                    idleSince =
                        reader.TokenType == JsonTokenType.Null ? null : reader.GetDateTimeOffset();
                    break;
                case "auto_delete":
                    reader.GetBoolean();
                    break;
                case "consumers":
                    reader.GetInt32();
                    break;
                default:
                    var value = JsonPrimitiveReader.ReadPrimitive(ref reader, options);
                    props.Add(key, value);
                    break;
            }
        }

        if (name is null)
        {
            throw new JsonException(
                $"Missing required property \"{nameof(QueueDetails.Name)}\" for model \"{nameof(QueueDetails)}\"."
            );
        }

        return new QueueDetails
        {
            Name = name,
            AdditionalProperties = props.ToImmutable(),
            AutoDelete = autoDelete,
            Consumers = consumers,
            IdleSince = idleSince,
            Messages = messages,
        };
    }

    public override void Write(
        Utf8JsonWriter writer,
        QueueDetails value,
        JsonSerializerOptions options
    )
    {
        throw new NotImplementedException();
    }
}
