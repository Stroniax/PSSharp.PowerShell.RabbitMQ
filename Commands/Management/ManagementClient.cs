using System.Diagnostics;
using System.Net;
using System.Net.Http.Headers;
using System.Net.Http.Json;
using System.Runtime.CompilerServices;
using System.Security.Authentication;
using System.Text;
using System.Text.Json.Serialization.Metadata;
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

    public async IAsyncEnumerable<Binding> ListExchangeBindingsBySource(
        string vhost,
        string fromExchange,
        [EnumeratorCancellation] CancellationToken cancellationToken
    )
    {
        ArgumentException.ThrowIfNullOrEmpty(vhost);
        ArgumentException.ThrowIfNullOrEmpty(fromExchange);

        var vhostPart = Uri.EscapeDataString(vhost);
        var exchangePart = Uri.EscapeDataString(fromExchange);

        using var response = await _http.GetAsync(
            $"/api/exchanges/{vhostPart}/{exchangePart}/bindings/source",
            cancellationToken
        );
        await CheckManagementHttpStatusCode(response, cancellationToken);

        await foreach (
            var row in response.Content.ReadFromJsonAsAsyncEnumerable(
                ManagementApiJsonContext.Default.Binding,
                cancellationToken
            )
        )
        {
            yield return row!;
        }
    }

    public async IAsyncEnumerable<Binding> ListExchangeBindingsByDestination(
        string vhost,
        string toExchange,
        [EnumeratorCancellation] CancellationToken cancellationToken
    )
    {
        ArgumentException.ThrowIfNullOrEmpty(vhost);
        ArgumentException.ThrowIfNullOrEmpty(toExchange);

        var vhostPart = Uri.EscapeDataString(vhost);
        var exchangePart = Uri.EscapeDataString(toExchange);

        using var response = await _http.GetAsync(
            $"/api/exchanges/{vhostPart}/{exchangePart}/bindings/destination",
            cancellationToken
        );
        await CheckManagementHttpStatusCode(response, cancellationToken);

        await foreach (
            var row in response.Content.ReadFromJsonAsAsyncEnumerable(
                ManagementApiJsonContext.Default.Binding,
                cancellationToken
            )
        )
        {
            yield return row!;
        }
    }

    public async IAsyncEnumerable<Binding> ListQueueBindings(
        string vhost,
        string toQueue,
        [EnumeratorCancellation] CancellationToken cancellationToken
    )
    {
        ArgumentException.ThrowIfNullOrEmpty(vhost);
        ArgumentException.ThrowIfNullOrEmpty(toQueue);

        var vhostPart = Uri.EscapeDataString(vhost);
        var queuePart = Uri.EscapeDataString(toQueue);

        using var response = await _http.GetAsync(
            $"/api/queues/{vhostPart}/{queuePart}/bindings",
            cancellationToken
        );
        await CheckManagementHttpStatusCode(response, cancellationToken);

        await foreach (
            var row in response.Content.ReadFromJsonAsAsyncEnumerable(
                ManagementApiJsonContext.Default.Binding,
                cancellationToken
            )
        )
        {
            yield return row!;
        }
    }

    public async IAsyncEnumerable<Binding> ListBindings(
        string? vhost,
        [EnumeratorCancellation] CancellationToken cancellationToken
    )
    {
        var vhostPart = vhost is null ? null : Uri.EscapeDataString(vhost);

        using var response = await _http.GetAsync($"/api/bindings/{vhostPart}", cancellationToken);
        await CheckManagementHttpStatusCode(response, cancellationToken);

        await foreach (
            var row in response.Content.ReadFromJsonAsAsyncEnumerable(
                ManagementApiJsonContext.Default.Binding,
                cancellationToken
            )
        )
        {
            yield return row!;
        }
    }

    public async IAsyncEnumerable<Binding> ListBindingsFromExchangeToQueue(
        string vhost,
        string exchange,
        string queue,
        [EnumeratorCancellation] CancellationToken cancellationToken
    )
    {
        ArgumentException.ThrowIfNullOrEmpty(vhost);
        ArgumentException.ThrowIfNullOrEmpty(exchange);
        ArgumentException.ThrowIfNullOrEmpty(queue);

        var vhostPart = Uri.EscapeDataString(vhost);
        var exchangePart = Uri.EscapeDataString(exchange);
        var queuePart = Uri.EscapeDataString(queue);

        using var response = await _http.GetAsync(
            $"/api/bindings/{vhostPart}/e/{exchangePart}/q/{queuePart}",
            cancellationToken
        );
        await CheckManagementHttpStatusCode(response, cancellationToken);

        await foreach (
            var row in response.Content.ReadFromJsonAsAsyncEnumerable(
                ManagementApiJsonContext.Default.Binding,
                cancellationToken
            )
        )
        {
            yield return row!;
        }
    }

    public async IAsyncEnumerable<Binding> ListBindingsFromExchangeToExchange(
        string vhost,
        string source,
        string destination,
        [EnumeratorCancellation] CancellationToken cancellationToken
    )
    {
        ArgumentException.ThrowIfNullOrEmpty(vhost);
        ArgumentException.ThrowIfNullOrEmpty(source);
        ArgumentException.ThrowIfNullOrEmpty(destination);

        var vhostPart = Uri.EscapeDataString(vhost);
        var sourcePart = Uri.EscapeDataString(source);
        var destinationPart = Uri.EscapeDataString(destination);

        using var response = await _http.GetAsync(
            $"/api/bindings/{vhostPart}/e/{sourcePart}/q/{destinationPart}",
            cancellationToken
        );
        await CheckManagementHttpStatusCode(response, cancellationToken);

        await foreach (
            var row in response.Content.ReadFromJsonAsAsyncEnumerable(
                ManagementApiJsonContext.Default.Binding,
                cancellationToken
            )
        )
        {
            yield return row!;
        }
    }

    public async Task<Binding?> GetExchangeBinding(
        string vhost,
        string source,
        string destination,
        string propertiesKey,
        CancellationToken cancellationToken
    )
    {
        ArgumentException.ThrowIfNullOrEmpty(vhost);
        ArgumentException.ThrowIfNullOrEmpty(source);
        ArgumentException.ThrowIfNullOrEmpty(destination);
        ArgumentException.ThrowIfNullOrEmpty(propertiesKey);

        var vhostPart = Uri.EscapeDataString(vhost);
        var sourcePart = Uri.EscapeDataString(source);
        var destinationPart = Uri.EscapeDataString(destination);
        var propsPart = Uri.EscapeDataString(propertiesKey);

        var uri = $"/api/bindings/{vhostPart}/e/{sourcePart}/e/{destinationPart}/{propsPart}";
        using var response = await _http.GetAsync(uri, cancellationToken);
        if (response.StatusCode == HttpStatusCode.NotFound)
        {
            return null;
        }
        await CheckManagementHttpStatusCode(response, cancellationToken);
        var data = await response.Content.ReadFromJsonAsync(
            ManagementApiJsonContext.Default.Binding,
            cancellationToken
        );
        return data!;
    }

    public async Task<Binding?> GetQueueBinding(
        string vhost,
        string exchange,
        string queue,
        string propertiesKey,
        CancellationToken cancellationToken
    )
    {
        ArgumentException.ThrowIfNullOrEmpty(vhost);
        ArgumentException.ThrowIfNullOrEmpty(exchange);
        ArgumentException.ThrowIfNullOrEmpty(queue);
        ArgumentException.ThrowIfNullOrEmpty(propertiesKey);

        var vhostPart = Uri.EscapeDataString(vhost);
        var sourcePart = Uri.EscapeDataString(exchange);
        var destinationPart = Uri.EscapeDataString(queue);
        var propsPart = Uri.EscapeDataString(propertiesKey);

        var uri = $"/api/bindings/{vhostPart}/e/{sourcePart}/e/{destinationPart}/{propsPart}";
        using var response = await _http.GetAsync(uri, cancellationToken);
        if (response.StatusCode == HttpStatusCode.NotFound)
        {
            return null;
        }
        await CheckManagementHttpStatusCode(response, cancellationToken);
        var data = await response.Content.ReadFromJsonAsync(
            ManagementApiJsonContext.Default.Binding,
            cancellationToken
        );
        return data!;
    }

    public async Task<bool> DeleteExchangeBinding(
        string vhost,
        string source,
        string destination,
        string propertiesKey,
        CancellationToken cancellationToken
    )
    {
        ArgumentException.ThrowIfNullOrEmpty(vhost);
        ArgumentException.ThrowIfNullOrEmpty(source);
        ArgumentException.ThrowIfNullOrEmpty(destination);
        ArgumentException.ThrowIfNullOrEmpty(propertiesKey);

        var vhostPart = Uri.EscapeDataString(vhost);
        var sourcePart = Uri.EscapeDataString(source);
        var destinationPart = Uri.EscapeDataString(destination);
        var propsPart = Uri.EscapeDataString(propertiesKey);

        var uri = $"/api/bindings/{vhostPart}/e/{sourcePart}/e/{destinationPart}/{propsPart}";
        using var response = await _http.GetAsync(uri, cancellationToken);
        if (response.StatusCode == HttpStatusCode.NotFound)
        {
            return false;
        }
        await CheckManagementHttpStatusCode(response, cancellationToken);
        return true;
    }

    public async Task<bool> DeleteQueueBinding(
        string vhost,
        string exchange,
        string queue,
        string propertiesKey,
        CancellationToken cancellationToken
    )
    {
        ArgumentException.ThrowIfNullOrEmpty(vhost);
        ArgumentException.ThrowIfNullOrEmpty(exchange);
        ArgumentException.ThrowIfNullOrEmpty(queue);
        ArgumentException.ThrowIfNullOrEmpty(propertiesKey);

        var vhostPart = Uri.EscapeDataString(vhost);
        var sourcePart = Uri.EscapeDataString(exchange);
        var destinationPart = Uri.EscapeDataString(queue);
        var propsPart = Uri.EscapeDataString(propertiesKey);

        var uri = $"/api/bindings/{vhostPart}/e/{sourcePart}/q/{destinationPart}/{propsPart}";
        using var response = await _http.GetAsync(uri, cancellationToken);
        if (response.StatusCode == HttpStatusCode.NotFound)
        {
            return false;
        }
        await CheckManagementHttpStatusCode(response, cancellationToken);
        return true;
    }

    public async Task<string> CreateExchangeBinding(
        string vhost,
        string source,
        string destination,
        CreateBinding payload,
        CancellationToken cancellationToken
    )
    {
        ArgumentException.ThrowIfNullOrEmpty(vhost);
        ArgumentException.ThrowIfNullOrEmpty(source);
        ArgumentException.ThrowIfNullOrEmpty(destination);

        var vhostPart = Uri.EscapeDataString(vhost);
        var sourcePart = Uri.EscapeDataString(source);
        var destinationPart = Uri.EscapeDataString(destination);

        using var response = await _http.PostAsJsonAsync(
            $"/api/bindings/{vhostPart}/e/{sourcePart}/e/{destinationPart}",
            payload,
            ManagementApiJsonContext.Default.CreateBinding,
            cancellationToken
        );
        await CheckManagementHttpStatusCode(response, cancellationToken);
        return GetLastPathPartOfLocation(response);
    }

    public async Task<string> CreateQueueBinding(
        string vhost,
        string exchange,
        string queue,
        CreateBinding payload,
        CancellationToken cancellationToken
    )
    {
        ArgumentException.ThrowIfNullOrEmpty(vhost);
        ArgumentException.ThrowIfNullOrEmpty(exchange);
        ArgumentException.ThrowIfNullOrEmpty(queue);

        var vhostPart = Uri.EscapeDataString(vhost);
        var sourcePart = Uri.EscapeDataString(exchange);
        var destinationPart = Uri.EscapeDataString(queue);

        using var response = await _http.PostAsJsonAsync(
            $"/api/bindings/{vhostPart}/e/{sourcePart}/q/{destinationPart}",
            payload,
            ManagementApiJsonContext.Default.CreateBinding,
            cancellationToken
        );
        await CheckManagementHttpStatusCode(response, cancellationToken);
        return GetLastPathPartOfLocation(response);
    }

    private static string GetLastPathPartOfLocation(HttpResponseMessage response)
    {
        if (response.Headers.Location is null)
        {
            throw new UnreachableException(
                "The API contractually sends a Location header in response to binding factory endpoints."
            );
        }

        var path = response.Headers.Location.AbsolutePath.AsSpan().TrimEnd('/');
        var lastPartIndex = path.LastIndexOf('/') + 1;
        return path[lastPartIndex..].ToString();
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

    public async Task<bool> DeleteExchange(
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
        if (response.StatusCode == HttpStatusCode.NotFound)
        {
            return false;
        }
        await CheckManagementHttpStatusCode(response, cancellationToken);
        return true;
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

    public async Task<ConnectionDetails?> GetConnection(
        string name,
        CancellationToken cancellationToken
    )
    {
        ArgumentException.ThrowIfNullOrEmpty(name);
        var namePart = Uri.EscapeDataString(name);
        using var response = await _http.GetAsync(
            $"/api/connections/{namePart}",
            cancellationToken
        );
        if (response.StatusCode == HttpStatusCode.NotFound)
        {
            return null;
        }
        await CheckManagementHttpStatusCode(response, cancellationToken);
        var content = await response.Content.ReadFromJsonAsync(
            ManagementApiJsonContext.Default.ConnectionDetails,
            cancellationToken
        );
        return content!;
    }

    public async IAsyncEnumerable<ConnectionDetails> ListConnections(
        [EnumeratorCancellation] CancellationToken cancellationToken
    )
    {
        await foreach (
            var row in ListConnectionsAtUri(
                "/api/connections",
                ManagementApiJsonContext.Default.ConnectionDetails,
                cancellationToken
            )
        )
        {
            yield return row;
        }
    }

    public async IAsyncEnumerable<ConnectionDetails> ListConnectionsByVhost(
        string vhost,
        [EnumeratorCancellation] CancellationToken cancellationToken
    )
    {
        ArgumentException.ThrowIfNullOrEmpty(vhost);
        var vhostPart = Uri.EscapeDataString(vhost);
        await foreach (
            var row in ListConnectionsAtUri(
                $"/api/vhosts/{vhostPart}/connections",
                ManagementApiJsonContext.Default.ConnectionDetails,
                cancellationToken
            )
        )
        {
            yield return row;
        }
    }

    public async IAsyncEnumerable<UserConnectionDetails> ListConnectionsByUser(
        string user,
        [EnumeratorCancellation] CancellationToken cancellationToken
    )
    {
        ArgumentException.ThrowIfNullOrEmpty(user);
        var userPart = Uri.EscapeDataString(user);
        await foreach (
            var row in ListConnectionsAtUri(
                $"/api/connections/username/{userPart}",
                ManagementApiJsonContext.Default.UserConnectionDetails,
                cancellationToken
            )
        )
        {
            yield return row;
        }
    }

    [StackTraceHidden]
    private async IAsyncEnumerable<T> ListConnectionsAtUri<T>(
        string uri,
        JsonTypeInfo<T> json,
        [EnumeratorCancellation] CancellationToken cancellationToken
    )
        where T : UserConnectionDetails
    {
        using var response = await _http.GetAsync(uri, cancellationToken);
        await CheckManagementHttpStatusCode(response, cancellationToken);
        await foreach (
            var row in response.Content.ReadFromJsonAsAsyncEnumerable(json, cancellationToken)
        )
        {
            yield return row!;
        }
    }

    public async Task DeleteConnectionsByUser(
        string user,
        string? reason,
        CancellationToken cancellationToken
    )
    {
        ArgumentException.ThrowIfNullOrEmpty(user);
        var userPart = Uri.EscapeDataString(user);
        using var request = new HttpRequestMessage(
            HttpMethod.Delete,
            $"/api/connections/username{userPart}"
        );
        if (reason is not null)
        {
            request.Headers.Add("X-Reason", reason);
        }
        using var response = await _http.SendAsync(request, cancellationToken);
        await CheckManagementHttpStatusCode(response, cancellationToken);
    }

    public async Task<bool> DeleteConnection(
        string name,
        string? reason,
        CancellationToken cancellationToken
    )
    {
        ArgumentNullException.ThrowIfNull(name);
        var namePart = Uri.EscapeDataString(name);
        using var request = new HttpRequestMessage(
            HttpMethod.Delete,
            $"/api/connections/{namePart}"
        );
        if (reason is not null)
        {
            request.Headers.Add("X-Reason", reason);
        }
        using var response = await _http.SendAsync(request, cancellationToken);
        if (response.StatusCode == HttpStatusCode.NotFound)
        {
            return false;
        }
        await CheckManagementHttpStatusCode(response, cancellationToken);
        return true;
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

internal static class ManagementClientExtensions
{
    /// <summary>
    /// Uses the optimal server-side endpoint to list bindings given a variety of filtering parameters. Filters
    /// that cannot be combined server-side will still run the "closest match" API endpoint and run the other
    /// filters client-side using case-sensitive comparison.
    /// </summary>
    /// <exception cref="ArgumentOutOfRangeException"></exception>
    public static async IAsyncEnumerable<Binding> ListBindings(
        this ManagementClient client,
        string? vhost = null,
        string? source = null,
        string? destination = null,
        DestinationType? destinationType = null,
        string? routingKey = null,
        string? propertiesKey = null,
        [EnumeratorCancellation] CancellationToken cancellationToken = default
    )
    {
        ArgumentNullException.ThrowIfNull(client);

        if (
            vhost is not null
            && source is not null
            && destination is not null
            && destinationType is not null
            && routingKey is not null
            && propertiesKey is not null
        )
        {
            var binding = destinationType switch
            {
                DestinationType.Exchange => await client.GetExchangeBinding(
                    vhost,
                    source,
                    destination,
                    propertiesKey,
                    cancellationToken
                ),
                DestinationType.Queue => await client.GetQueueBinding(
                    vhost,
                    source,
                    destination,
                    propertiesKey,
                    cancellationToken
                ),
                _ => throw new ArgumentOutOfRangeException(
                    nameof(destinationType),
                    destinationType,
                    "The value is not a defined enumeration value."
                ),
            };
            if (binding is not null)
            {
                yield return binding;
            }
            yield break;
        }

        // When a filter is "consumed" server-side, set it to null to avoid the client from repeating the comparison
        IAsyncEnumerable<Binding> serverFiltered;
        if (vhost is null)
        {
            serverFiltered = client.ListBindings(null, cancellationToken);
        }
        else if (
            source is not null
            && destination is not null
            && destinationType == DestinationType.Exchange
        )
        {
            serverFiltered = client.ListBindingsFromExchangeToExchange(
                vhost,
                source,
                destination,
                cancellationToken
            );
            vhost = null;
            source = null;
            destination = null;
            destinationType = null;
        }
        else if (
            source is not null
            && destination is not null
            && destinationType == DestinationType.Queue
        )
        {
            serverFiltered = client.ListBindingsFromExchangeToQueue(
                vhost,
                source,
                destination,
                cancellationToken
            );
            vhost = null;
            source = null;
            destination = null;
            destinationType = null;
        }
        else if (source is not null && destination is not null)
        {
            // [From exchange] to [both queue and exchanges].
            // Therefore we can't use "to destination" because that only goes "to destination of type exchange"
            // "by source" is likely to be extremely broad, so I think it is better to make two specific network requests
            var e2e = client.ListBindingsFromExchangeToExchange(
                vhost,
                source,
                destination,
                cancellationToken
            );
            var e2q = client.ListBindingsFromExchangeToQueue(
                vhost,
                source,
                destination,
                cancellationToken
            );
            vhost = null;
            source = null;
            destination = null;

            serverFiltered = e2e.Concat(e2q);
        }
        else if (destination is not null && destinationType == DestinationType.Exchange)
        {
            serverFiltered = client.ListExchangeBindingsByDestination(
                vhost,
                destination,
                cancellationToken
            );
            vhost = null;
            destination = null;
        }
        else if (destination is not null && destinationType == DestinationType.Queue)
        {
            serverFiltered = client.ListQueueBindings(vhost, destination, cancellationToken);
            vhost = null;
            destination = null;
            destinationType = null;
        }
        else if (source is not null)
        {
            serverFiltered = client.ListExchangeBindingsBySource(vhost, source, cancellationToken);
            vhost = null;
            source = null;
        }
        else
        {
            serverFiltered = client.ListBindings(vhost, cancellationToken);
            vhost = null;
        }

        await foreach (var binding in serverFiltered)
        {
            if (vhost is not null && !binding.VHost.Equals(vhost))
            {
                continue;
            }

            if (source is not null && !binding.Source.Equals(source))
            {
                continue;
            }

            if (destination is not null && !binding.Destination.Equals(destination))
            {
                continue;
            }

            if (destinationType is not null && binding.DestinationType != destinationType)
            {
                continue;
            }

            if (routingKey is not null && !binding.RoutingKey.Equals(routingKey))
            {
                continue;
            }

            if (propertiesKey is not null && !binding.PropertiesKey.Equals(propertiesKey))
            {
                continue;
            }

            yield return binding;
        }
    }
}
