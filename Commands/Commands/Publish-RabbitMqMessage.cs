using System.Collections;
using System.Management.Automation;
using System.Text;
using Microsoft.PowerShell.Commands;
using Newtonsoft.Json;
using RabbitMQ.Client;

namespace PSSharp.PowerShell.RabbitMQ.Commands;

[Cmdlet(VerbsData.Publish, Noun, SupportsShouldProcess = true)]
public sealed class PublishRabbitMqMessageCommand : RmqCmdlet
{
    private const string Noun = "RabbitMqMessage";

    [Parameter(ValueFromPipelineByPropertyName = true)]
    [QueueNameArgumentCompleter]
    public string RoutingKey { get; set; } = string.Empty;

    [Parameter(ValueFromPipelineByPropertyName = true)]
    [ArgumentCompletions("''")]
    public string Exchange { get; set; } = string.Empty;

    [Parameter(ValueFromPipeline = true, Mandatory = true)]
    [AllowNull, AllowEmptyString, AllowEmptyCollection]
    [Alias("InputObject")]
    [ArgumentCompletions("$_")]
    public PSObject? Payload { get; set; }

    [Parameter]
    public PSRabbitMessageSerialization Serialization { get; set; } =
        PSRabbitMessageSerialization.Clixml;

    [Parameter(Mandatory = true)]
    [RabbitConnectionCompleter]
    [RabbitConnection]
    public PSRabbitConnection Connection { get; set; } = null!;

    [Parameter]
    [ArgumentCompletions("''")]
    public string? AppId { get; set; }

    /// <summary>
    /// Inferred based on <see cref="Serialization"/> if not specified.
    /// </summary>
    [Parameter]
    [ArgumentCompletions("text/plain")]
    public string? ContentType { get; set; }

    [Parameter]
    [ArgumentCompletions("utf-8")]
    public string? ContentEncoding { get; set; }

    [Parameter]
    [ArgumentCompletions("''")]
    public string? ClusterId { get; set; }

    [Parameter]
    [ArgumentCompletions("''")]
    public string? CorrelationId { get; set; }

    [Parameter]
    [ArgumentCompletions("''")]
    public string? MessageId { get; set; }

    [Parameter]
    public DeliveryModes DeliveryMode { get; set; }

    [Parameter]
    [ValidateRange("00:00:00", "10675199.02:48:05.4775807")] // TimeSpan.MaxValue
    [ArgumentCompletions("00:00:05", "00:05:00", "05:00:00")]
    public TimeSpan? Expiration { get; set; }

    [Parameter]
    public SwitchParameter Persistent { get; set; }

    [Parameter]
    [ValidateRange(0, 9)]
    [ArgumentCompletions("0", "1", "2", "3", "4", "5", "6", "7", "8", "9")]
    public byte Priority { get; set; }

    [Parameter]
    [ArgumentCompletions("''")]
    public string? Type { get; set; }

    [Parameter]
    [ArgumentCompletions("''")]
    public string? UserId { get; set; }

    [Parameter]
    [ArgumentCompletions("(Get-Date)")]
    public DateTime Timestamp { get; set; } = DateTime.Now;

    [Parameter]
    [ArgumentCompletions("@{}")]
    public Hashtable? Headers { get; set; }

    [Parameter]
    [ArgumentCompletions("$null", "\"${ExchangeType}://$ExchangeName/$RoutingKey\"")]
    public PublicationAddress? ReplyTo { get; set; }

    /// <summary>
    /// Wait for the message to be routed. Raise an error if the message cannot be routed.
    /// </summary>
    [Parameter]
    public SwitchParameter Mandatory { get; set; }

    private Dictionary<string, object?>? HeadersDictionary()
    {
        // Cannot cache the dictionary because the parameter might change as the pipeline updates.
        if (Headers is null)
        {
            return null;
        }
        var r = new Dictionary<string, object?>();
        foreach (DictionaryEntry entry in Headers)
        {
            if (entry.Key is not string key)
            {
                throw new ArgumentException("Header keys must be strings.", nameof(Headers));
            }
            r[key] = entry.Value;
        }
        return r;
    }

    protected override async Task ProcessRecordAsync()
    {
        var channel = await Connection.GetDefaultChannel(PipelineStopToken).ConfigureAwait(false);

        string? contentType = null;
        byte[] body;
        try
        {
            (contentType, body) = Serialization switch
            {
                PSRabbitMessageSerialization.Clixml => (
                    "application/clixml+xml",
                    Encoding.UTF8.GetBytes(PSSerializer.Serialize(Payload))
                ),
                PSRabbitMessageSerialization.Json => (
                    "application/json",
                    Encoding.UTF8.GetBytes(
                        JsonObject.ConvertToJson(
                            Payload,
                            new(
                                100,
                                true,
                                true,
                                StringEscapeHandling.EscapeNonAscii,
                                this,
                                PipelineStopToken
                            )
                        )
                    )
                ),
                PSRabbitMessageSerialization.Utf8 => (
                    "text/plain",
                    Encoding.UTF8.GetBytes(
                        LanguagePrimitives.ConvertTo<string>(Payload) ?? string.Empty
                    )
                ),
                PSRabbitMessageSerialization.Binary => (
                    "application/octet-stream",
                    LanguagePrimitives.ConvertTo<byte[]>(Payload)
                ),
                _ => throw new NotSupportedException(
                    $"Serialization type {Serialization} is not supported."
                ),
            };
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            var er = new ErrorRecord(ex, "SerializationFailed", ErrorCategory.InvalidData, Payload);
            await WriteErrorAsync(er);
            return;
        }

        var basicProperties = new BasicProperties()
        {
            AppId = AppId,
            ClusterId = ClusterId,
            ContentType = ContentType ?? contentType,
            ContentEncoding = ContentEncoding,
            CorrelationId = CorrelationId,
            DeliveryMode = DeliveryMode,
            Expiration = Expiration?.Milliseconds.ToString(),
            MessageId = MessageId,
            Persistent = Persistent.IsPresent,
            Priority = Priority,
            Type = Type,
            UserId = UserId,
            Timestamp = new AmqpTimestamp(new DateTimeOffset(Timestamp).ToUnixTimeSeconds()),
            Headers = HeadersDictionary(),
            ReplyToAddress = ReplyTo,
        };

        if (
            !await InvokeAsync(() =>
                    ShouldProcess(
                        $"payload {body.Length} byte(s) to exchange {Exchange}, routing key {RoutingKey}",
                        "publish message"
                    )
                )
                .ConfigureAwait(false)
        )
        {
            return;
        }

        try
        {
            await channel.BasicPublishAsync(
                Exchange,
                RoutingKey,
                Mandatory.IsPresent,
                basicProperties,
                body,
                PipelineStopToken
            );
        }
        catch (Exception ex)
        {
            var er = new ErrorRecord(ex, "PublishFailed", ErrorCategory.ConnectionError, Payload);
            await WriteErrorAsync(er);
        }
    }
}
