using System.Collections;
using System.Management.Automation;
using System.Text;
using Microsoft.PowerShell.Commands;
using Newtonsoft.Json;
using RabbitMQ.Client;

namespace PSSharp.PowerShell.RabbitMQ;

[Cmdlet(VerbsData.Publish, Noun, SupportsShouldProcess = true)]
public sealed class PublishRabbitMqMessageCommand : RmqCmdlet
{
    private const string Noun = "RabbitMqMessage";

    [Parameter(ValueFromPipelineByPropertyName = true)]
    public string RoutingKey { get; set; } = string.Empty;

    [Parameter(ValueFromPipelineByPropertyName = true)]
    public string Exchange { get; set; } = string.Empty;

    [Parameter(ValueFromPipeline = true, Mandatory = true)]
    [AllowNull, AllowEmptyString, AllowEmptyCollection]
    [Alias("InputObject")]
    public PSObject? Payload { get; set; }

    [Parameter]
    public PSRabbitMessageSerialization Serialization { get; set; } = PSRabbitMessageSerialization.Clixml;

    [Parameter(Mandatory = true)]
    [RabbitConnectionCompleter]
    [RabbitConnection]
    public PSRabbitConnection Connection { get; set; } = null!;

    [Parameter]
    public string? AppId { get; set; }

    /// <summary>
    /// Inferred based on <see cref="Serialization"/> if not specified.
    /// </summary>
    [Parameter]
    public string? ContentType { get; set; }

    [Parameter]
    public string? ContentEncoding { get; set; }

    [Parameter]
    public string? ClusterId { get; set; }

    [Parameter]
    public string? CorrelationId { get; set; }

    [Parameter]
    public string? MessageId { get; set; }

    [Parameter]
    public DeliveryModes DeliveryMode { get; set; }

    [Parameter]
    [ValidateRange("00:00:00", "10675199.02:48:05.4775807")] // TimeSpan.MaxValue
    public TimeSpan? Expiration { get; set; }

    [Parameter]
    public SwitchParameter Persistent { get; set; }

    [Parameter]
    [ValidateRange(0, 9)]
    public byte Priority { get; set; }

    [Parameter]
    public string? Type { get; set; }

    [Parameter]
    public string? UserId { get; set; }

    [Parameter]
    public DateTime Timestamp { get; set; } = DateTime.Now;

    [Parameter]
    public Hashtable? Headers { get; set; }

    [Parameter]
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
        var channel = await Connection.GetDefaultChannel(StoppingToken).ConfigureAwait(false);

        string? contentType = null;
        byte[] body;
        try
        {
            (contentType, body) = Serialization switch
            {
                PSRabbitMessageSerialization.Clixml => ("application/clixml+xml", Encoding.UTF8.GetBytes(PSSerializer.Serialize(Payload))),
                PSRabbitMessageSerialization.Json => ("application/json", Encoding.UTF8.GetBytes(
                    JsonObject.ConvertToJson(Payload, new(100, true, true, StringEscapeHandling.EscapeNonAscii, this, PipelineStopToken))
                )),
                PSRabbitMessageSerialization.Utf8 => ("text/plain", Encoding.UTF8.GetBytes(LanguagePrimitives.ConvertTo<string>(Payload) ?? string.Empty)),
                PSRabbitMessageSerialization.Binary => ("application/octet-stream", LanguagePrimitives.ConvertTo<byte[]>(Payload)),
                _ => throw new NotSupportedException($"Serialization type {Serialization} is not supported.")
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

        if (!await InvokeAsync(() => ShouldProcess($"payload {body.Length} byte(s) to exchange {Exchange}, routing key {RoutingKey}", "publish message")).ConfigureAwait(false))
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
                StoppingToken
            );
        }
        catch (Exception ex)
        {
            var er = new ErrorRecord(ex, "PublishFailed", ErrorCategory.ConnectionError, Payload);
            await WriteErrorAsync(er);
        }
    }
}
