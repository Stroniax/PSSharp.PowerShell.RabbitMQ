using System.Text.Json;
using System.Text.Json.Serialization;
using RabbitMQ.Client;

namespace PSSharp.PowerShell.RabbitMQ.Commands.Management;

internal sealed class BasicPropertiesJsonConverter : JsonConverter<IReadOnlyBasicProperties>
{
    public override IReadOnlyBasicProperties? Read(
        ref Utf8JsonReader reader,
        Type typeToConvert,
        JsonSerializerOptions options
    )
    {
        var props = new BasicProperties();

        if (reader.TokenType != JsonTokenType.StartObject)
        {
            throw new JsonException("Expected StartObject.");
        }

        while (true)
        {
            reader.Read();

            if (reader.TokenType == JsonTokenType.EndObject)
            {
                break;
            }
            else if (reader.TokenType == JsonTokenType.PropertyName)
            {
                var propertyName = reader.GetString();
                reader.Read();
                switch (propertyName)
                {
                    case "app_id":
                        props.AppId = reader.GetString();
                        break;
                    case "cluster_id":
                        props.ClusterId = reader.GetString();
                        break;
                    case "content_encoding":
                        props.ContentEncoding = reader.GetString();
                        break;
                    case "content_type":
                        props.ContentType = reader.GetString();
                        break;
                    case "correlation_id":
                        props.CorrelationId = reader.GetString();
                        break;
                    case "delivery_mode":
                        props.DeliveryMode = (DeliveryModes)reader.GetInt32();
                        break;
                    case "headers":
                        props.Headers = JsonSerializer.Deserialize<Dictionary<string, object?>>(
                            ref reader,
                            options
                        );
                        break;
                    case "message_id":
                        props.MessageId = reader.GetString();
                        break;
                    case "priority":
                        props.Priority = reader.GetByte();
                        break;
                    case "reply_to":
                        props.ReplyTo = reader.GetString();
                        break;
                    case "timestamp":
                        props.Timestamp = new(reader.GetInt64());
                        break;
                    case "type":
                        props.Type = reader.GetString();
                        break;
                    case "user_id":
                        props.UserId = reader.GetString();
                        break;
                    default:
                        // ignore unknown members
                        break;
                }
            }
            else
            {
                throw new JsonException("Unexpected json token. Expected property name.");
            }
        }

        return props;
    }

    public override void Write(
        Utf8JsonWriter writer,
        IReadOnlyBasicProperties value,
        JsonSerializerOptions options
    )
    {
        throw new NotImplementedException();
    }
}
