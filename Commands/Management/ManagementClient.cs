using System.Net;
using System.Net.Http.Headers;
using System.Net.Http.Json;
using System.Runtime.CompilerServices;
using System.Security.Authentication;
using System.Text;
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

    public async Task<PublishResponse> PublishMessage(
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

        var body = new PublishRequest
        {
            Properties = properties,
            RoutingKey = routingKey,
            Payload = messagePayload,
            PayloadEncoding = "base64",
        };

        var vhostPart = Uri.EscapeDataString(vhost);
        var exchangePart = Uri.EscapeDataString(exchange);
        using var response = await _http
            .PostAsJsonAsync(
                $"/api/exchanges/{vhostPart}/{exchangePart}/publish",
                body,
                ManagementApiJsonContext.Default.PublishRequest,
                cancellationToken
            )
            .ConfigureAwait(false);

        await CheckManagementHttpStatusCode(response, cancellationToken).ConfigureAwait(false);
        var responseContent = await response.Content.ReadFromJsonAsync(
            ManagementApiJsonContext.Default.PublishResponse,
            cancellationToken
        );
        return responseContent!;
    }

    public async IAsyncEnumerable<Message> GetMessages(
        string vhost,
        string queueName,
        ulong count,
        string ackmode,
        string encoding,
        [EnumeratorCancellation] CancellationToken cancellationToken
    )
    {
        ArgumentException.ThrowIfNullOrEmpty(vhost);
        ArgumentException.ThrowIfNullOrEmpty(queueName);

        var vhostPart = Uri.EscapeDataString(vhost);
        var queuePart = Uri.EscapeDataString(queueName);
        var path = $"/api/queues/{vhostPart}/{queuePart}/get";

        using var response = await _http.PostAsJsonAsync(
            path,
            new MessageRequest
            {
                Count = count,
                AckMode = ackmode,
                Encoding = encoding,
            },
            ManagementApiJsonContext.Default.MessageRequest,
            cancellationToken
        );

        await CheckManagementHttpStatusCode(response, cancellationToken);

        await foreach (
            var row in response.Content.ReadFromJsonAsAsyncEnumerable(
                ManagementApiJsonContext.Default.Message,
                cancellationToken
            )
        )
        {
            yield return row!;
        }
    }

    public async IAsyncEnumerable<QueueDetails> ListQueues(
        string? vhost,
        [EnumeratorCancellation] CancellationToken cancellationToken
    )
    {
        var vhostPart = vhost is null ? string.Empty : Uri.EscapeDataString(vhost);

        await foreach (
            var row in _http.GetFromJsonAsAsyncEnumerable(
                $"/api/queues/{vhostPart}",
                ManagementApiJsonContext.Default.QueueDetails,
                cancellationToken
            )
        )
        {
            yield return row!;
        }
    }

    public async Task DeclareQueue(
        string vhost,
        string queue,
        QueueDeclareOptions args,
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
            ManagementApiJsonContext.Default.QueueDeclareOptions,
            cancellationToken
        );

        await CheckManagementHttpStatusCode(response, cancellationToken);
    }

    public async Task<QueueDetails> GetQueue(
        string vhost,
        string queue,
        CancellationToken cancellationToken
    )
    {
        ArgumentException.ThrowIfNullOrEmpty(vhost);
        ArgumentException.ThrowIfNullOrEmpty(queue);

        var vhostPart = Uri.EscapeDataString(vhost);
        var queuePart = Uri.EscapeDataString(queue);

        using var response = await _http.GetAsync(
            $"/api/queues/{vhostPart}/{queuePart}",
            cancellationToken
        );

        await CheckManagementHttpStatusCode(response, cancellationToken);

        var data = await response.Content.ReadFromJsonAsync(
            ManagementApiJsonContext.Default.QueueDetails,
            cancellationToken
        );
        return data!;
    }

    /// <returns><see langword="true"/> if the queue existed, <see langword="false"/> if it was <see cref="HttpStatusCode.NotFound"/>.</returns>
    public async Task<bool> DeleteQueue(
        string vhost,
        string queue,
        DeleteQueueOptions options,
        CancellationToken cancellationToken
    )
    {
        ArgumentException.ThrowIfNullOrEmpty(vhost);
        ArgumentException.ThrowIfNullOrEmpty(queue);

        var vhostPart = Uri.EscapeDataString(vhost);
        var queuePart = Uri.EscapeDataString(queue);

        var ifEmpty = options.IfEmpty ? "true" : "";
        var ifUnused = options.IfUnused ? "true" : "";
        using var response = await _http.DeleteAsync(
            $"/api/queues/{vhostPart}/{queuePart}?if-empty={ifEmpty}&if-unused={ifUnused}",
            cancellationToken
        );

        if (response.StatusCode == HttpStatusCode.NotFound)
        {
            return false;
        }

        await CheckManagementHttpStatusCode(response, cancellationToken);
        return true;
    }

    public async Task PurgeQueue(string vhost, string queue, CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrEmpty(vhost);
        ArgumentException.ThrowIfNullOrEmpty(queue);

        var vhostPart = Uri.EscapeDataString(vhost);
        var queuePart = Uri.EscapeDataString(queue);

        using var response = await _http.DeleteAsync(
            $"/api/queues/{vhostPart}/{queuePart}/contents",
            cancellationToken
        );

        await CheckManagementHttpStatusCode(response, cancellationToken);
    }

    public async IAsyncEnumerable<ExchangeDetails> ListExchanges(
        string? vhost,
        [EnumeratorCancellation] CancellationToken cancellationToken
    )
    {
        var vhostPart = vhost is null ? null : Uri.EscapeDataString(vhost);

        using var response = await _http.GetAsync($"/api/exchanges/{vhostPart}", cancellationToken);

        await CheckManagementHttpStatusCode(response, cancellationToken);

        await foreach (
            var row in response.Content.ReadFromJsonAsAsyncEnumerable(
                ManagementApiJsonContext.Default.ExchangeDetails,
                cancellationToken
            )
        )
        {
            yield return row!;
        }
    }

    public async Task<ExchangeDetails?> GetExchange(
        string vhost,
        string exchangeName,
        CancellationToken cancellationToken
    )
    {
        ArgumentException.ThrowIfNullOrEmpty(vhost);
        ArgumentNullException.ThrowIfNull(exchangeName);

        var vhostPart = Uri.EscapeDataString(vhost);
        var exchangePart = exchangeName is "" ? "amq.default" : Uri.EscapeDataString(exchangeName);

        using var response = await _http.GetAsync(
            $"/api/exchanges/{vhostPart}/{exchangePart}",
            cancellationToken
        );

        if (response.StatusCode == HttpStatusCode.NotFound)
        {
            return null;
        }

        await CheckManagementHttpStatusCode(response, cancellationToken);

        var content = await response.Content.ReadFromJsonAsync(
            ManagementApiJsonContext.Default.ExchangeDetails,
            cancellationToken
        );
        return content;
    }

    public async Task DeclareExchange(
        string vhost,
        string exchangeName,
        ExchangeDeclareOptions options,
        CancellationToken cancellationToken
    )
    {
        ArgumentException.ThrowIfNullOrEmpty(vhost);
        ArgumentException.ThrowIfNullOrEmpty(exchangeName);

        var vhostPart = Uri.EscapeDataString(vhost);
        var exchangePart = Uri.EscapeDataString(exchangeName);
        var path = $"/api/exchanges/{vhostPart}/{exchangePart}";

        using var response = await _http.PutAsJsonAsync(
            path,
            options,
            ManagementApiJsonContext.Default.ExchangeDeclareOptions,
            cancellationToken
        );

        await CheckManagementHttpStatusCode(response, cancellationToken);
    }

    public async Task DeleteExchange(
        string vhost,
        string exchangeName,
        DeleteExchangeOptions options,
        CancellationToken cancellationToken
    )
    {
        ArgumentException.ThrowIfNullOrEmpty(vhost);
        ArgumentException.ThrowIfNullOrEmpty(exchangeName);

        var vhostPart = Uri.EscapeDataString(vhost);
        var exchangePart = Uri.EscapeDataString(exchangeName);
        var ifUnused = options.IfUnused ? "true" : "false";
        var uri = $"/api/exchanges/{vhostPart}/{exchangePart}?if-unused={ifUnused}";

        using var response = await _http.DeleteAsync(uri, cancellationToken);
        await CheckManagementHttpStatusCode(response, cancellationToken);
    }

    public async IAsyncEnumerable<VirtualHost> ListVirtualHosts(
        [EnumeratorCancellation] CancellationToken cancellationToken
    )
    {
        using var response = await _http.GetAsync("/api/vhosts", cancellationToken);

        await CheckManagementHttpStatusCode(response, cancellationToken);

        await foreach (
            var row in response.Content.ReadFromJsonAsAsyncEnumerable(
                ManagementApiJsonContext.Default.VirtualHost,
                cancellationToken
            )
        )
        {
            yield return row!;
        }
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
                var description = await response.Content.ReadFromJsonAsync(
                    ManagementApiJsonContext.Default.ApiError,
                    cancellationToken
                );
                throw new InvalidOperationException(
                    $"The RabbitMQ management API returned status code 400 Bad Request: {description?.Reason}."
                )
                {
                    Data =
                    {
                        ["StatusCode"] = 400,
                        ["Error"] = description?.Error,
                        ["Reason"] = description?.Reason,
                    },
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
}
