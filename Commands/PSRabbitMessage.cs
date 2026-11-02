using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Management.Automation;
using System.Reflection.Metadata.Ecma335;
using System.Text;
using Microsoft.PowerShell.Commands;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace PSSharp.PowerShell.RabbitMQ.Commands;

public sealed record PSRabbitMessage
{
    public PSRabbitMessage(
        PSObject? body,
        string exchange,
        string routingKey,
        ulong deliveryTag,
        bool redelivered,
        IReadOnlyBasicProperties basicProperties,
        byte[] rawBody
    )
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

    internal static bool FromGet(
        BasicGetResult get,
        PSRabbitMessageSerialization? serialization,
        [MaybeNullWhen(false)] out PSRabbitMessage message,
        [MaybeNullWhen(true)] out ErrorRecord error
    )
    {
        serialization ??= get.BasicProperties.ContentType switch
        {
            "application/octet-stream" => PSRabbitMessageSerialization.Binary,
            "application/json" or "text/json" => PSRabbitMessageSerialization.Json,
            "text/xml" or "application/xml" or "application/clixml+xml" =>
                PSRabbitMessageSerialization.Clixml,
            "text/plain" => PSRabbitMessageSerialization.Utf8,
            // Try to deserialize as UTF8 if there is no other indication in the payload. The user can still use the RawBody if needed
            null or "" => PSRabbitMessageSerialization.Utf8,
            _ => PSRabbitMessageSerialization.Binary,
        };

        PSObject? body;
        switch (serialization)
        {
            case PSRabbitMessageSerialization.Utf8:
                var utf8 = Encoding.UTF8.GetString(get.Body.Span);
                body = new PSObject(utf8);
                break;
            case PSRabbitMessageSerialization.Binary:
                body = new PSObject(get.Body.ToArray());
                break;
            case PSRabbitMessageSerialization.Json:
                var json = Encoding.UTF8.GetString(get.Body.Span);
                var jsonBody = JsonObject.ConvertFromJson(json, out error);
                body = new PSObject(jsonBody);
                if (error is not null)
                {
                    message = null;
                    return false;
                }
                break;
            case PSRabbitMessageSerialization.Clixml:
                var clixml = Encoding.UTF8.GetString(get.Body.Span);
                var xmlBody = PSSerializer.Deserialize(clixml);
                body = new PSObject(xmlBody);
                break;
            default:
                throw new UnreachableException("No serialization is defined.");
        }

        message = new PSRabbitMessage(
            body,
            get.Exchange,
            get.RoutingKey,
            get.DeliveryTag,
            get.Redelivered,
            get.BasicProperties,
            get.Body.ToArray()
        )
        {
            MessageCount = get.MessageCount,
        };
        error = null;
        return true;
    }

    internal static bool FromDeliver(
        BasicDeliverEventArgs deliver,
        PSRabbitMessageSerialization? serialization,
        [MaybeNullWhen(false)] out PSRabbitMessage message,
        [MaybeNullWhen(true)] out ErrorRecord error
    )
    {
        serialization ??= deliver.BasicProperties.ContentType switch
        {
            "application/octet-stream" => PSRabbitMessageSerialization.Binary,
            "application/json" or "text/json" => PSRabbitMessageSerialization.Json,
            "text/xml" or "application/xml" or "application/clixml+xml" =>
                PSRabbitMessageSerialization.Clixml,
            "text/plain" => PSRabbitMessageSerialization.Utf8,
            // Try to deserialize as UTF8 if there is no other indication in the payload. The user can still use the RawBody if needed
            null or "" => PSRabbitMessageSerialization.Utf8,
            _ => PSRabbitMessageSerialization.Binary,
        };

        PSObject? body;
        switch (serialization)
        {
            case PSRabbitMessageSerialization.Utf8:
                var utf8 = Encoding.UTF8.GetString(deliver.Body.Span);
                body = new PSObject(utf8);
                break;
            case PSRabbitMessageSerialization.Binary:
                body = new PSObject(deliver.Body.ToArray());
                break;
            case PSRabbitMessageSerialization.Json:
                var json = Encoding.UTF8.GetString(deliver.Body.Span);
                var jsonBody = JsonObject.ConvertFromJson(json, out error);
                body = new PSObject(jsonBody);
                if (error is not null)
                {
                    message = null;
                    return false;
                }
                break;
            case PSRabbitMessageSerialization.Clixml:
                var clixml = Encoding.UTF8.GetString(deliver.Body.ToArray());
                var xmlBody = PSSerializer.Deserialize(clixml);
                body = new PSObject(xmlBody);
                break;
            default:
                throw new UnreachableException("No serialization is defined.");
        }

        message = new PSRabbitMessage(
            body,
            deliver.Exchange,
            deliver.RoutingKey,
            deliver.DeliveryTag,
            deliver.Redelivered,
            deliver.BasicProperties,
            deliver.Body.ToArray()
        );
        error = null;
        return true;
    }
}
