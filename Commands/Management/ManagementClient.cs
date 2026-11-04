using System.Net;
using System.Net.Http.Headers;
using System.Net.Http.Json;
using System.Security.Authentication;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using RabbitMQ.Client;

namespace PSSharp.PowerShell.RabbitMQ.Commands.Management;

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

    public async Task<ManagementPublishResponse> PublishMessage(
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

    public async Task<ManagementGetResponse[]> GetMessages(
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

    public async Task DeclareQueue(
        string vhost,
        string queue,
        ManagementQueueDeclare args,
        CancellationToken cancellationToken
    )
    {
        ArgumentException.ThrowIfNullOrEmpty(vhost);
        ArgumentException.ThrowIfNullOrEmpty(queue);

        var vhostPart = Uri.EscapeDataString(vhost);
        var queuePart = Uri.EscapeDataString(queue);

        using var response = await _http.PutAsJsonAsync(
            $"/api/queues/{vhostPart}/{queuePart}",
            args,
            MANAGEMENT_JSON_OPTIONS,
            cancellationToken
        );

        await CheckManagementHttpStatusCode(response, cancellationToken);
    }

    public async Task<ManagementQueueResponse> GetQueue(
        string vhost,
        string queue,
        CancellationToken cancellationToken
    )
    {
        var vhostPart = Uri.EscapeDataString(vhost);
        var queuePart = Uri.EscapeDataString(queue);

        using var response = await _http.GetAsync(
            $"/api/queues/{vhostPart}/{queuePart}",
            cancellationToken
        );

        var data = await response.Content.ReadFromJsonAsync<ManagementQueueResponse>(
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
                $"The RabbitMQ management API returned an unsuccessful status code "
                    + $"{(int)response.StatusCode} ({response.StatusCode}): {response.ReasonPhrase}. "
                    + $"The response content follows: {content}",
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
