using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Management.Automation;
using System.Text;
using Microsoft.PowerShell.Commands;
using PSSharp.PowerShell.RabbitMQ.Commands.Management;
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

    /// <summary>
    /// Might be <see langword="null"/> according to the deserialized message body, or if there was an error
    /// parsing the message payload. Use <see cref="RawBody"/> if necessary or observe errors of the cmdlet
    /// or job that produced this record.
    /// </summary>
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

        private const int RESPOND_NONE = 0;
        private const int RESPOND_IN_PROGRESS = 1;
        private const int RESPOND_SENT = 2;

        private int _responded = RESPOND_NONE;

        internal IChannel Channel { get; }
        public ulong DeliveryTag { get; }

        public bool HasResponded() => _responded == RESPOND_SENT;

        public async Task AckAsync(CancellationToken cancellationToken)
        {
            using var trackResponseStatus = StartRespond();
            await Channel.BasicAckAsync(DeliveryTag, false, cancellationToken);
        }

        public async Task NackAsync(bool requeue, CancellationToken cancellationToken)
        {
            using var trackResponseStatus = StartRespond();
            await Channel.BasicNackAsync(DeliveryTag, false, requeue, cancellationToken);
        }

        public async Task RejectAsync(bool requeue, CancellationToken cancellationToken)
        {
            using var trackResponseStatus = StartRespond();
            await Channel.BasicRejectAsync(DeliveryTag, requeue, cancellationToken);
        }

        private RespondInProgress StartRespond()
        {
            var was = Interlocked.CompareExchange(
                ref _responded,
                RESPOND_IN_PROGRESS,
                RESPOND_NONE
            );

            return was switch
            {
                RESPOND_NONE => new(this),
                RESPOND_IN_PROGRESS => throw new InvalidOperationException(
                    "The message is presently being responded to. Be sure to await the Task returned from Ack/Nack/Reject and do not call another operation on the same instance concurrently."
                ),
                RESPOND_SENT => throw new InvalidOperationException(
                    "The message has already been responded to."
                ),
                _ => throw new UnreachableException(),
            };
        }

        private struct RespondInProgress(InFlightPSRabbitMessage p) : IDisposable
        {
            private int _isDisposed;

            public void Dispose()
            {
                if (Interlocked.CompareExchange(ref _isDisposed, 1, 0) == 0)
                {
                    Interlocked.CompareExchange(
                        ref p._responded,
                        RESPOND_SENT,
                        RESPOND_IN_PROGRESS
                    );
                }
            }
        }
    }

    internal static bool FromGet(
        IChannel? channel,
        BasicGetResult get,
        PSRabbitMessageSerialization? serialization,
        out PSRabbitMessage message,
        [MaybeNullWhen(true)] out ErrorRecord error
    )
    {
        serialization ??= ResolveSerialization(get.BasicProperties.ContentType);

        ParseBody(serialization.Value, get.Body, out var body, out error);

        if (channel is null)
        {
            message = new(
                body,
                get.Exchange,
                get.RoutingKey,
                get.Redelivered,
                get.BasicProperties,
                get.Body.ToArray()
            )
            {
                MessageCount = get.MessageCount,
            };
        }
        else
        {
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
        }
        return error is null;
    }

    internal static bool FromDeliver(
        IChannel? channel,
        BasicDeliverEventArgs deliver,
        PSRabbitMessageSerialization? serialization,
        out PSRabbitMessage message,
        [MaybeNullWhen(true)] out ErrorRecord error
    )
    {
        serialization ??= ResolveSerialization(deliver.BasicProperties.ContentType);

        ParseBody(serialization.Value, deliver.Body, out var body, out error);

        if (channel is null)
        {
            message = new(
                body,
                deliver.Exchange,
                deliver.RoutingKey,
                deliver.Redelivered,
                deliver.BasicProperties,
                deliver.Body.ToArray()
            );
        }
        else
        {
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
        }
        return error is null;
    }

    internal static bool FromManagement(
        Message get,
        PSRabbitMessageSerialization? serialization,
        out PSRabbitMessage message,
        [MaybeNullWhen(true)] out ErrorRecord error
    )
    {
        serialization ??= ResolveSerialization(get.Properties.ContentType);

        var rawPayload = Convert.FromBase64String(get.Payload);
        ParseBody(serialization.Value, rawPayload, out var body, out error);

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
        return error is null;
    }

    private static readonly UTF8Encoding StrictUtf8 = new(false, true);

    /// <remarks>
    /// The error occurs during deserialization, which is why callers still construct a
    /// <see cref="PSRabbitMessage"/> (with a <see langword="null"/> body) if the parse fails. This
    /// allows callers to ack/nack the message which they must do for manual ack mode anyway, so it
    /// would be improper to propagate the error and discard the message.
    /// </remarks>
    private static bool ParseBody(
        PSRabbitMessageSerialization serialization,
        ReadOnlyMemory<byte> body,
        out PSObject? parsedBody,
        [MaybeNullWhen(true)] out ErrorRecord error
    )
    {
        try
        {
            return ParseCore(serialization, body, out parsedBody, out error);
        }
        catch (Exception ex)
        {
            parsedBody = null;
            error = new ErrorRecord(
                ex,
                "MesssagePayloadDeserializationError",
                ErrorCategory.InvalidData,
                null
            );
            return false;
        }
    }

    private static bool ParseCore(
        PSRabbitMessageSerialization serialization,
        ReadOnlyMemory<byte> body,
        out PSObject? parsedBody,
        [MaybeNullWhen(true)] out ErrorRecord error
    )
    {
        error = default;
        switch (serialization)
        {
            case PSRabbitMessageSerialization.Utf8:
                try
                {
                    var utf8 = StrictUtf8.GetString(body.Span);
                    parsedBody = new PSObject(utf8);
                }
                catch (ArgumentException ex)
                {
                    error = new ErrorRecord(
                        ex,
                        "NotUtf8",
                        ErrorCategory.InvalidData,
                        body.ToArray()
                    );
                    parsedBody = null;
                }
                break;
            case PSRabbitMessageSerialization.Binary:
                parsedBody = new PSObject(body.ToArray());
                break;
            case PSRabbitMessageSerialization.Json:
                var json = Encoding.UTF8.GetString(body.Span);
                var jsonBody = JsonObject.ConvertFromJson(json, out error);
                parsedBody = jsonBody is null ? null : PSObject.AsPSObject(jsonBody);
                break;
            case PSRabbitMessageSerialization.Clixml:
                var clixml = Encoding.UTF8.GetString(body.Span);
                var xmlBody = PSSerializer.Deserialize(clixml);
                parsedBody = xmlBody is null ? null : PSObject.AsPSObject(xmlBody);
                break;
            default:
                throw new UnreachableException("No or invalid serialization is specified.");
        }

        return error is null;
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
