using System.Collections;
using System.Diagnostics;
using System.Management.Automation;
using System.Net;
using System.Net.Http.Headers;
using System.Net.Http.Json;
using System.Security.Authentication;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.PowerShell.Commands;
using Newtonsoft.Json;
using RabbitMQ.Client;

namespace PSSharp.PowerShell.RabbitMQ.Commands;

[Cmdlet(
    VerbsCommunications.Send,
    Noun,
    SupportsShouldProcess = true,
    DefaultParameterSetName = AmqpParameterSet,
    RemotingCapability = RemotingCapability.OwnedByCommand
)]
[Alias("sdrmq")]
public sealed class SendRabbitMqMessageCommand : RmqCmdlet
{
    private const string Noun = "RabbitMqMessage";
    private const string AmqpParameterSet = "Amqp";
    private const string ManagementParameterSet = "Management";

    [Parameter(ValueFromPipeline = true, Mandatory = true, Position = 0)]
    [AllowNull, AllowEmptyString, AllowEmptyCollection]
    [Alias("InputObject")]
    [ArgumentCompletions("$_")]
    public PSObject? Payload { get; set; }

    [Parameter(ValueFromPipelineByPropertyName = true, Position = 1)]
    [ArgumentCompletions("''")]
    public string Exchange { get; set; } = string.Empty;

    [Parameter(ValueFromPipelineByPropertyName = true, Position = 2)]
    [QueueNameCompleter]
    public string RoutingKey { get; set; } = string.Empty;

    [Parameter(ValueFromPipelineByPropertyName = true, Position = 3)]
    [ArgumentCompletions("@{}")]
    public Hashtable? Headers { get; set; }

    [Parameter(ValueFromPipelineByPropertyName = true, Position = 4)]
    public PSRabbitMessageSerialization Serialization { get; set; } =
        PSRabbitMessageSerialization.Clixml;

    [Parameter(Mandatory = true, Position = 5, ParameterSetName = AmqpParameterSet)]
    [RabbitConnectionCompleter]
    [RabbitConnection]
    public PSRabbitConnection? Connection { get; set; }

    [Parameter(Mandatory = true, Position = 5, ParameterSetName = ManagementParameterSet)]
    [ValidateAbsoluteUri]
    [ValidateUriScheme("http", "https")]
    [ManagementServerUriCompleter]
    public Uri? ManagementUri { get; set; }

    [Parameter(Position = 6, ParameterSetName = ManagementParameterSet)]
    [Credential]
    [ArgumentCompletions("guest")]
    public PSCredential? Credential { get; set; }

    [Parameter(Position = 7, ParameterSetName = ManagementParameterSet)]
    [ValidateNotNullOrEmpty]
    [ArgumentCompletions("/")]
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

        var uri = ManagementUri;
        NetworkCredential? cred = null;
        if (uri.UserInfo is not "")
        {
            var userPortion = uri.UserInfo.Split(':', 2);
            uri = new UriBuilder(uri) { UserName = "", Password = "" }.Uri;
            cred = new NetworkCredential(
                userPortion[0],
                userPortion.Length == 2 ? Uri.UnescapeDataString(userPortion[1]) : string.Empty
            );
        }
        if (Credential is not null)
        {
            cred = Credential.GetNetworkCredential();
        }

        if (cred is null)
        {
            var ex = new ArgumentException("A credential must be provided for the Management API.");
            var er = new ErrorRecord(
                ex,
                "CredentialRequired",
                ErrorCategory.AuthenticationError,
                ManagementUri
            );
            await WriteErrorAsync(er).ConfigureAwait(false);
            return;
        }

        using var http = new HttpClient()
        {
            BaseAddress = uri,
            DefaultRequestHeaders =
            {
                Authorization = new AuthenticationHeaderValue(
                    "Basic",
                    Convert.ToBase64String(
                        Encoding.ASCII.GetBytes($"{cred.UserName}:{cred.Password}")
                    )
                ),
            },
        };

        SerializeBody(out var contentType, out var bytes);
        var payload = Convert.ToBase64String(bytes);

        var body = new
        {
            properties = new
            {
                app_id = AppId,
                cluster_id = ClusterId,
                content_type = ContentType ?? contentType,
                content_encoding = ContentEncoding,
                correlation_id = CorrelationId,
                delivery_mode = DeliveryMode,
                expiration = Expiration?.Milliseconds.ToString(),
                message_id = MessageId,
                priority = Priority,
                type = Type,
                user_id = UserId,
                timestamp = new DateTimeOffset(Timestamp).ToUnixTimeSeconds(),
                headers = HeadersDictionary(),
                reply_to = ReplyTo,
            },
            routing_key = RoutingKey,
            payload,
            payload_encoding = "base64",
        };

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

        var vhost = Uri.EscapeDataString(VHost);
        var name = Uri.EscapeDataString(Exchange == "" ? "amq.default" : Exchange);
        using var response = await http.PostAsJsonAsync(
                $"/api/exchanges/{vhost}/{name}/publish",
                body,
                MANAGEMENT_JSON_OPTIONS,
                PipelineStopToken
            )
            .ConfigureAwait(false);

        if (!await CheckManagementHttpStatusCode(response).ConfigureAwait(false))
        {
            return;
        }

        if (Mandatory)
        {
            var responseContent = await response
                .Content.ReadFromJsonAsync<JsonElement>(PipelineStopToken)
                .ConfigureAwait(false);
            if (!responseContent.GetProperty("routed").GetBoolean())
            {
                var ex = new Exception(
                    "The published message was mandatory, but was not routed to any queue."
                );
                var er = new ErrorRecord(
                    ex,
                    "NotRouted",
                    ErrorCategory.InvalidResult,
                    responseContent
                );
                await WriteErrorAsync(er).ConfigureAwait(false);
            }
        }
    }

    private async Task<bool> CheckManagementHttpStatusCode(HttpResponseMessage response)
    {
        switch (response.StatusCode)
        {
            case HttpStatusCode.Unauthorized:
                {
                    var ex = new AuthenticationException(
                        "The RabbitMQ management API returned status code 401 Unauthorized."
                    );
                    var er = new ErrorRecord(
                        ex,
                        "ManagementUnauthorized",
                        ErrorCategory.AuthenticationError,
                        ManagementUri
                    );
                    await WriteErrorAsync(er);
                }
                return false;
            case HttpStatusCode.BadRequest:
                {
                    var ex = new InvalidOperationException(
                        "The RabbitMQ management API returned status code 400 Bad Request."
                    );
                    var er = new ErrorRecord(
                        ex,
                        "ManagementBadRequest",
                        ErrorCategory.NotSpecified,
                        response.RequestMessage?.Content
                    );
                    await WriteErrorAsync(er);
                }
                return false;
            case HttpStatusCode.Forbidden:
            {
                var ex = new UnauthorizedAccessException(
                    "The RabbitMQ management API returned status code 403 Forbidden."
                );
                var er = new ErrorRecord(
                    ex,
                    "ManagementForbidden",
                    ErrorCategory.PermissionDenied,
                    ManagementUri
                );
                await WriteErrorAsync(er);
                return false;
            }
        }

        if (!response.IsSuccessStatusCode)
        {
            var content = await response.Content.ReadAsStringAsync(PipelineStopToken);
            var ex = new HttpRequestException(
                $"The RabbitMQ management API returned an unsuccessful status code. The response content follows: {content}",
                null,
                response.StatusCode
            );
            var er = new ErrorRecord(
                ex,
                "UnsuccessfulStatusCode",
                ErrorCategory.NotSpecified,
                response
            );
            await WriteErrorAsync(er);
            return false;
        }

        return true;
    }

    private async Task PublishAmqp()
    {
        Debug.Assert(
            Connection is not null,
            "The 'Connection' parameter is mandatory for the amqp* parameter set."
        );

        var channel = await Connection.GetDefaultChannel(PipelineStopToken).ConfigureAwait(false);

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

    /// <summary>
    /// Null cmdlet parameters need to be not included. RabbitMQ does not support the
    /// json literal `null`, rather it expects those members to be not present.
    /// </summary>
    private static readonly JsonSerializerOptions MANAGEMENT_JSON_OPTIONS = new(
        JsonSerializerDefaults.Web
    )
    {
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
    };
}
