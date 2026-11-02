using Microsoft.PowerShell.Commands;
using RabbitMQ.Client;
using System.Diagnostics;
using System.Management.Automation;
using System.Text;

namespace PSSharp.PowerShell.RabbitMQ.Commands;

public sealed record PSRabbitMessage
{
    public PSRabbitMessage(PSObject? body, string exchange, string routingKey, ulong deliveryTag, bool redelivered, IReadOnlyBasicProperties basicProperties, byte[] rawBody)
    {
        Body = body;
        Exchange = exchange;
        RoutingKey = routingKey;
        DeliveryTag = deliveryTag;
        Redelivered = redelivered;
        BasicProperties = basicProperties;
        RawBody = rawBody;
    }
    public PSObject? Body { get; init; }
    public string Exchange { get; init; }
    public string RoutingKey { get; init; }
    public ulong DeliveryTag { get; init; }
    public bool Redelivered { get; init; }
    public IReadOnlyBasicProperties BasicProperties { get; init; }
    public byte[] RawBody { get; init; }

    public uint? MessageCount { get; init; }

    internal static async Task<(PSRabbitMessage?, ErrorRecord?)> FromGet(
        BasicGetResult get,
        PSRabbitMessageSerialization? serialization,
        CancellationToken cancellationToken)
    {
        serialization ??= get.BasicProperties.ContentType switch
        {
            "application/octet-stream" => PSRabbitMessageSerialization.Binary,
            "application/json" or "text/json" => PSRabbitMessageSerialization.Json,
            "text/xml" or "application/xml" or "application/clixml+xml" => PSRabbitMessageSerialization.Clixml,
            "text/plain" => PSRabbitMessageSerialization.Utf8,
            // Try to deserialize as UTF8 if there is no other indication in the payload. The user can still use the RawBody if needed
            null or "" => PSRabbitMessageSerialization.Utf8,
            _ => PSRabbitMessageSerialization.Binary,
        };

        PSObject? body;
        switch (serialization)
        {
            case PSRabbitMessageSerialization.Utf8:
                var utf8 = Encoding.UTF8.GetString(get.Body.ToArray());
                body = new PSObject(utf8);
                break;
            case PSRabbitMessageSerialization.Binary:
                body = new PSObject(get.Body.ToArray());
                break;
            case PSRabbitMessageSerialization.Json:
                var json = Encoding.UTF8.GetString(get.Body.ToArray());
                var jsonBody = JsonObject.ConvertFromJson(json, out var error);
                body = new PSObject(jsonBody);
                if (error is not null)
                {
                    return (null, error);
                }
                break;
            case PSRabbitMessageSerialization.Clixml:
                var clixml = Encoding.UTF8.GetString(get.Body.ToArray());
                var xmlBody = PSSerializer.Deserialize(clixml);
                body = new PSObject(xmlBody);
                break;
            default:
                throw new UnreachableException("No serialization is defined.");
        }

        var message = new PSRabbitMessage(
            body,
            get.Exchange,
            get.RoutingKey,
            get.DeliveryTag,
            get.Redelivered,
            get.BasicProperties,
            get.Body.ToArray()
            )
        {
          MessageCount =  get.MessageCount
        };
        return (message, null);
    }
}