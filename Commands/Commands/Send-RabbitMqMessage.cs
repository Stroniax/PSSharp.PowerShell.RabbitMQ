using System.Collections;
using System.Diagnostics;
using System.Management.Automation;
using System.Security.Authentication;
using System.Text;
using Microsoft.PowerShell.Commands;
using Newtonsoft.Json;
using PSSharp.PowerShell.RabbitMQ.Commands.Management;
using RabbitMQ.Client;

namespace PSSharp.PowerShell.RabbitMQ.Commands;

[Cmdlet(
    VerbsCommunications.Send,
    Nouns.RabbitMqMessage,
    SupportsShouldProcess = true,
    DefaultParameterSetName = AmqpParameterSet,
    RemotingCapability = RemotingCapability.OwnedByCommand
)]
[Alias("sdrmq")]
public sealed class SendRabbitMqMessageCommand : RmqCmdlet
{
    private const string AmqpParameterSet = "Amqp";
    private const string ManagementParameterSet = "Management";

    [Parameter(ValueFromPipeline = true, Mandatory = true, Position = 0)]
    [AllowNull, AllowEmptyString, AllowEmptyCollection]
    [Alias("InputObject")]
    [ArgumentCompletions("$_")]
    public PSObject? Payload { get; set; }

    [Parameter(ValueFromPipelineByPropertyName = true, Position = 1)]
    [QueueNameCompleter]
    public string RoutingKey { get; set; } = string.Empty;

    [Parameter(ValueFromPipelineByPropertyName = true, Position = 2)]
    [ExchangeNameCompleter]
    public string Exchange { get; set; } = string.Empty;

    [Parameter(ValueFromPipelineByPropertyName = true, Position = 3)]
    [ArgumentCompletions("@{}")]
    public Hashtable? Headers { get; set; }

    [Parameter(ValueFromPipelineByPropertyName = true, Position = 4)]
    public PSRabbitMessageSerialization Serialization { get; set; } =
        PSRabbitMessageSerialization.Clixml;

    [Parameter(Mandatory = true, Position = 5, ParameterSetName = AmqpParameterSet)]
    [Alias("Connection")]
    [PSRabbitSessionCompleter]
    [PSRabbitSession]
    public PSRabbitSession? Session { get; set; }

    [Parameter(Mandatory = true, Position = 5, ParameterSetName = ManagementParameterSet)]
    [ManagementServerUriCompleter]
    [ValidateAbsoluteUri]
    [ValidateUriScheme("http", "https")]
    public Uri? ManagementUri { get; set; }

    [Parameter(Position = 6, ParameterSetName = ManagementParameterSet)]
    [Credential]
    [ArgumentCompletions("guest")]
    public PSCredential? Credential { get; set; }

    [Parameter(
        Position = 7,
        ParameterSetName = ManagementParameterSet,
        ValueFromPipelineByPropertyName = true
    )]
    [Alias("VirtualHost")]
    [ValidateNotNullOrEmpty]
    [VirtualHostCompleter]
    public string VHost { get; set; } = "/";

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

    private static bool _hasNotifiedManagementInefficient;

    protected override async Task BeginProcessingAsync()
    {
        if (!_hasNotifiedManagementInefficient && ParameterSetName == ManagementParameterSet)
        {
            _hasNotifiedManagementInefficient = true;
            await InvokeAsync(WarnManagementNotEfficient);
        }

        await base.BeginProcessingAsync();
    }

    private void WarnManagementNotEfficient()
    {
        WriteWarning(
            """
            Note that the HTTP API is a highly inefficient option for publishing;

            Prefer AMQP 1.0, AMQP 0-9-1, the RabbitMQ Streaming Protocol or any other messaging protocol supported by RabbitMQ.

            This alert is provided once when publishing using the -ManagementUri parameter per PowerShell session.
            """
        );
    }

    protected override async Task ProcessRecordAsync()
    {
        switch (ParameterSetName)
        {
            case AmqpParameterSet:
                await PublishAmqp();
                break;
            case ManagementParameterSet:
                await PublishManagement();
                break;
        }
    }

    private async Task PublishManagement()
    {
        Debug.Assert(
            ManagementUri is not null,
            "The 'ManagementUri' parameter is mandatory for the management* parameter set."
        );

        var cred = Credential?.GetNetworkCredential() ?? ManagementUri.GetNetworkCredential();
        var uri = ManagementUri.WithoutCredential();

        if (cred is null)
        {
            await this.ManagementCredentialRequiredError(ManagementUri);
            return;
        }

        using var client = new ManagementClient(uri, cred);

        SerializeBody(out var contentType, out var bytes);
        var basicProperties = BuildBasicProperties(contentType);

        if (
            !await InvokeAsync(() =>
                    ShouldProcess(
                        $"payload {bytes.Length} byte(s) to exchange {Exchange}, routing key {RoutingKey}",
                        "publish message"
                    )
                )
                .ConfigureAwait(false)
        )
        {
            return;
        }

        PublishResponse response;
        try
        {
            response = await client.PublishMessage(
                VHost,
                Exchange,
                RoutingKey,
                basicProperties,
                bytes,
                PipelineStopToken
            );
        }
        catch (AuthenticationException ex)
        {
            var er = new ErrorRecord(
                ex,
                "ManagementUnauthorized",
                ErrorCategory.AuthenticationError,
                ManagementUri
            );
            await WriteErrorAsync(er);
            return;
        }
        catch (InvalidOperationException ex) when (ex.Data["StatusCode"] is 400)
        {
            var er = new ErrorRecord(
                ex,
                "ManagementBadRequest",
                ErrorCategory.NotSpecified,
                Payload
            );
            await WriteErrorAsync(er);
            return;
        }
        catch (UnauthorizedAccessException ex)
        {
            var er = new ErrorRecord(
                ex,
                "ManagementForbidden",
                ErrorCategory.PermissionDenied,
                ManagementUri
            );
            await WriteErrorAsync(er);
            return;
        }
        catch (HttpRequestException ex)
        {
            var er = new ErrorRecord(
                ex,
                "UnsuccessfulStatusCode",
                ErrorCategory.NotSpecified,
                Payload
            );
            await WriteErrorAsync(er);
            return;
        }

        if (Mandatory && !response.Routed)
        {
            var ex = new Exception(
                "The published message was mandatory, but was not routed to any queue."
            );
            var er = new ErrorRecord(ex, "NotRouted", ErrorCategory.InvalidResult, Payload);
            await WriteErrorAsync(er).ConfigureAwait(false);
        }
    }

    private async Task PublishAmqp()
    {
        Debug.Assert(
            Session is not null,
            "The 'Session' parameter is mandatory for the amqp* parameter set."
        );

        var channel = await Session.GetDefaultChannel(PipelineStopToken).ConfigureAwait(false);

        string? contentType = null;
        byte[] body;
        try
        {
            SerializeBody(out contentType, out body);
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

        var basicProperties = BuildBasicProperties(contentType);

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

    private BasicProperties BuildBasicProperties(string contentType)
    {
        return new BasicProperties()
        {
            AppId = AppId,
            ClusterId = ClusterId,
            ContentType = ContentType ?? contentType,
            ContentEncoding = ContentEncoding,
            CorrelationId = CorrelationId,
            DeliveryMode = DeliveryMode,
            Expiration = Expiration?.Milliseconds.ToString(),
            MessageId = MessageId,
            Priority = Priority,
            Type = Type,
            UserId = UserId,
            Timestamp = new AmqpTimestamp(new DateTimeOffset(Timestamp).ToUnixTimeSeconds()),
            Headers = HeadersDictionary(),
            ReplyToAddress = ReplyTo,
        };
    }

    private void SerializeBody(out string contentType, out byte[] body)
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
}
