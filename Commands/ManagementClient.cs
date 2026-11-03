using System.Management.Automation;
using System.Net;
using System.Net.Http.Headers;
using System.Net.Http.Json;
using System.Security.Authentication;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using RabbitMQ.Client;

namespace PSSharp.PowerShell.RabbitMQ.Commands;

internal sealed class ManagementClient(HttpClient http) : IDisposable
{
    private readonly HttpClient _http = http;

    public ManagementClient(Uri baseUri, NetworkCredential credential)
        : this(new HttpClient { BaseAddress = baseUri })
    {
        if (!baseUri.IsAbsoluteUri)
        {
            throw new ArgumentException("The base URI must be absolute.", nameof(baseUri));
        }
        if (baseUri.UserInfo != "")
        {
            throw new ArgumentException(
                "The base URI must not include a credential. RabbitMQ management API supports Basic authorization.",
                nameof(baseUri)
            );
        }

        _http.DefaultRequestHeaders.Authorization = BasicHeader(credential);
    }

    private static AuthenticationHeaderValue BasicHeader(NetworkCredential credential)
    {
        var concat = $"{credential.UserName}:{credential.Password}";
        var bin = Encoding.ASCII.GetBytes(concat);
        var b64 = Convert.ToBase64String(bin);
        return new("Basic", b64);
    }

    public async Task<ManagementPublishResponse> PublishAsync(
        string vhost,
        string exchange,
        string routingKey,
        IReadOnlyBasicProperties properties,
        byte[] payload,
        CancellationToken cancellationToken
    )
    {
        if (string.IsNullOrWhiteSpace(exchange))
        {
            exchange = "amq.default";
        }

        var messagePayload = Convert.ToBase64String(payload);

        var body = new
        {
            properties = new
            {
                app_id = properties.AppId,
                cluster_id = properties.ClusterId,
                content_type = properties.ContentType,
                content_encoding = properties.ContentEncoding,
                correlation_id = properties.CorrelationId,
                delivery_mode = properties.DeliveryMode,
                expiration = properties.Expiration,
                message_id = properties.MessageId,
                priority = properties.Priority,
                type = properties.Type,
                user_id = properties.UserId,
                timestamp = properties.Timestamp.UnixTime,
                headers = properties.Headers,
                reply_to = properties.ReplyTo,
            },
            routing_key = routingKey,
            payload = messagePayload,
            payload_encoding = "base64",
        };

        var vhostPart = Uri.EscapeDataString(vhost);
        var exchangePart = Uri.EscapeDataString(exchange);
        using var response = await _http
            .PostAsJsonAsync(
                $"/api/exchanges/{vhostPart}/{exchangePart}/publish",
                body,
                MANAGEMENT_JSON_OPTIONS,
                cancellationToken
            )
            .ConfigureAwait(false);

        await CheckManagementHttpStatusCode(response, cancellationToken).ConfigureAwait(false);
        var responseContent = await response.Content.ReadFromJsonAsync<ManagementPublishResponse>(
            cancellationToken
        );
        return responseContent!;
    }

    public async Task<ManagementGetResponse[]> GetAsync(
        string vhost,
        string queueName,
        ulong count,
        string ackmode,
        string encoding,
        CancellationToken cancellationToken
    )
    {
        var vhostPart = Uri.EscapeDataString(vhost);
        var queuePart = Uri.EscapeDataString(queueName);
        var path = $"/api/queues/{vhostPart}/{queuePart}/get";

        using var response = await _http.PostAsJsonAsync(
            path,
            new
            {
                count,
                ackmode,
                encoding,
            },
            MANAGEMENT_JSON_OPTIONS,
            cancellationToken
        );

        await CheckManagementHttpStatusCode(response, cancellationToken);

        var data = await response.Content.ReadFromJsonAsync<ManagementGetResponse[]>(
            cancellationToken
        );
        return data!;
    }

    private static async Task CheckManagementHttpStatusCode(
        HttpResponseMessage response,
        CancellationToken cancellationToken
    )
    {
        switch (response.StatusCode)
        {
            case HttpStatusCode.Unauthorized:
                throw new AuthenticationException(
                    "The RabbitMQ management API returned status code 401 Unauthorized."
                )
                {
                    Data = { ["StatusCode"] = 401 },
                };
                ;
            case HttpStatusCode.BadRequest:
                throw new InvalidOperationException(
                    "The RabbitMQ management API returned status code 400 Bad Request."
                )
                {
                    Data = { ["StatusCode"] = 400 },
                };

            case HttpStatusCode.Forbidden:
                throw new UnauthorizedAccessException(
                    "The RabbitMQ management API returned status code 403 Forbidden."
                )
                {
                    Data = { ["StatusCode"] = 403 },
                };
        }

        if (!response.IsSuccessStatusCode)
        {
            var content = await response.Content.ReadAsStringAsync(cancellationToken);
            throw new HttpRequestException(
                $"The RabbitMQ management API returned an unsuccessful status code. The response content follows: {content}",
                null,
                response.StatusCode
            );
        }
    }

    public void Dispose()
    {
        _http.Dispose();
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

internal sealed record ManagementPublishResponse
{
    public bool Routed { get; init; }
}

internal sealed record ManagementGetResponse
{
    [JsonPropertyName("payload_bytes")]
    public ulong PayloadBytes { get; init; }

    [JsonPropertyName("redelivered")]
    public bool Redelivered { get; init; }

    [JsonPropertyName("exchange")]
    public string Exchange { get; set; } = string.Empty;

    [JsonPropertyName("routing_key")]
    public string RoutingKey { get; set; } = string.Empty;

    [JsonPropertyName("message_count")]
    public uint MessageCount { get; set; }

    [JsonPropertyName("properties")]
    [JsonConverter(typeof(BasicPropertiesJsonConverter))]
    public IReadOnlyBasicProperties Properties { get; init; } = new BasicProperties();

    [JsonPropertyName("payload")]
    public string Payload { get; init; } = string.Empty;

    [JsonPropertyName("payload_encoding")]
    public string PayloadEncoding { get; init; } = string.Empty;
}

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

internal static class CredentialExtensions
{
    extension(Uri @this)
    {
        public NetworkCredential? GetNetworkCredential()
        {
            if (string.IsNullOrEmpty(@this.UserInfo))
            {
                return null;
            }

            var parts = @this.UserInfo.Split(':', 2);
            var user = parts[0];
            var pass = parts.Length == 2 ? parts[1] : string.Empty;

            return new NetworkCredential(
                Uri.UnescapeDataString(user),
                Uri.UnescapeDataString(pass)
            );
        }

        public Uri WithoutCredential()
        {
            return new UriBuilder(@this) { UserName = "", Password = "" }.Uri;
        }
    }

    extension(RmqCmdlet @this)
    {
        public async Task ManagementCredentialRequiredError(Uri managementUri)
        {
            var ex = new ArgumentException("A credential must be provided for the Management API.");
            var er = new ErrorRecord(
                ex,
                "CredentialRequired",
                ErrorCategory.AuthenticationError,
                managementUri
            );
            await @this.WriteErrorAsync(er).ConfigureAwait(false);
        }
    }
}
