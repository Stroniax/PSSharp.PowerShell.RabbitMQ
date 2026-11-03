using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Management.Automation;
using System.Text;
using Microsoft.PowerShell.Commands;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace PSSharp.PowerShell.RabbitMQ.Commands;

/// <summary>
/// <para>
/// PowerShell-oriented representation of a RabbitMQ message.
/// </para>
/// <para>
/// The instance may implement <see cref="IInFlightMessage"/> if it was produced from a broker.
/// </para>
/// </summary>
public record PSRabbitMessage
{
    private PSRabbitMessage(
        PSObject? body,
        string exchange,
        string routingKey,
        bool redelivered,
        IReadOnlyBasicProperties basicProperties,
        byte[] rawBody
    )
    {
        Body = body;
        Exchange = exchange;
        RoutingKey = routingKey;
        Redelivered = redelivered;
        BasicProperties = basicProperties;
        RawBody = rawBody;
    }

    public PSObject? Body { get; }
    public string Exchange { get; }
    public string RoutingKey { get; }
    public bool Redelivered { get; }
    public IReadOnlyBasicProperties BasicProperties { get; }
    public byte[] RawBody { get; }
    public uint? MessageCount { get; init; }

    private sealed record InFlightPSRabbitMessage : PSRabbitMessage, IInFlightMessage
    {
        internal InFlightPSRabbitMessage(
            IChannel channel,
            PSObject? body,
            string exchange,
            string routingKey,
            ulong deliveryTag,
            bool redelivered,
            IReadOnlyBasicProperties basicProperties,
            byte[] rawBody
        )
            : base(body, exchange, routingKey, redelivered, basicProperties, rawBody)
        {
            Channel = channel;
            DeliveryTag = deliveryTag;
        }

        internal IChannel Channel { get; }
        public ulong DeliveryTag { get; }

        public async Task AckAsync(CancellationToken cancellationToken)
        {
            await Channel.BasicAckAsync(DeliveryTag, false, cancellationToken);
        }

        public async Task NackAsync(bool requeue, CancellationToken cancellationToken)
        {
            await Channel.BasicNackAsync(DeliveryTag, false, requeue, cancellationToken);
        }

        public async Task RejectAsync(bool requeue, CancellationToken cancellationToken)
        {
            await Channel.BasicRejectAsync(DeliveryTag, requeue, cancellationToken);
        }
    }

    internal static bool FromGet(
        IChannel channel,
        BasicGetResult get,
        PSRabbitMessageSerialization? serialization,
        [MaybeNullWhen(false)] out PSRabbitMessage message,
        [MaybeNullWhen(true)] out ErrorRecord error
    )
    {
        serialization ??= ResolveSerialization(get.BasicProperties.ContentType);

        if (!ParseBody(serialization.Value, get.Body, out var body, out error))
        {
            message = null;
            return false;
        }

        message = new InFlightPSRabbitMessage(
            channel,
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
        return true;
    }

    internal static bool FromDeliver(
        IChannel channel,
        BasicDeliverEventArgs deliver,
        PSRabbitMessageSerialization? serialization,
        [MaybeNullWhen(false)] out PSRabbitMessage message,
        [MaybeNullWhen(true)] out ErrorRecord error
    )
    {
        serialization ??= ResolveSerialization(deliver.BasicProperties.ContentType);

        if (!ParseBody(serialization.Value, deliver.Body, out var body, out error))
        {
            message = null;
            return false;
        }

        message = new InFlightPSRabbitMessage(
            channel,
            body,
            deliver.Exchange,
            deliver.RoutingKey,
            deliver.DeliveryTag,
            deliver.Redelivered,
            deliver.BasicProperties,
            deliver.Body.ToArray()
        );
        return true;
    }

    internal static bool FromManagement(
        ManagementGetResponse get,
        PSRabbitMessageSerialization? serialization,
        [MaybeNullWhen(false)] out PSRabbitMessage message,
        [MaybeNullWhen(true)] out ErrorRecord error
    )
    {
        serialization ??= ResolveSerialization(get.Properties.ContentType);

        var rawPayload = Convert.FromBase64String(get.Payload);
        if (!ParseBody(serialization.Value, rawPayload, out var body, out error))
        {
            message = null;
            return false;
        }

        message = new PSRabbitMessage(
            body,
            get.Exchange,
            get.RoutingKey,
            get.Redelivered,
            get.Properties,
            rawPayload
        )
        {
            MessageCount = get.MessageCount,
        };
        return true;
    }

    private static bool ParseBody(
        PSRabbitMessageSerialization serialization,
        ReadOnlyMemory<byte> body,
        out PSObject? parsedBody,
        [MaybeNullWhen(true)] out ErrorRecord error
    )
    {
        switch (serialization)
        {
            case PSRabbitMessageSerialization.Utf8:
                var utf8 = Encoding.UTF8.GetString(body.Span);
                parsedBody = new PSObject(utf8);
                break;
            case PSRabbitMessageSerialization.Binary:
                parsedBody = new PSObject(body.ToArray());
                break;
            case PSRabbitMessageSerialization.Json:
                var json = Encoding.UTF8.GetString(body.Span);
                var jsonBody = JsonObject.ConvertFromJson(json, out error);
                parsedBody = PSObject.AsPSObject(jsonBody);
                if (error is not null)
                {
                    return false;
                }
                break;
            case PSRabbitMessageSerialization.Clixml:
                var clixml = Encoding.UTF8.GetString(body.Span);
                var xmlBody = PSSerializer.Deserialize(clixml);
                parsedBody = new PSObject(xmlBody);
                break;
            default:
                throw new UnreachableException("No serialization is defined.");
        }

        error = default;
        return true;
    }

    private static PSRabbitMessageSerialization ResolveSerialization(string? contentType)
    {
        return contentType switch
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
    }
}
