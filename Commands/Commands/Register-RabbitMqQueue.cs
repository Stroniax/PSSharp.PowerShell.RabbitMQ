using System.Collections;
using System.Diagnostics;
using System.Management.Automation;
using PSSharp.PowerShell.RabbitMQ.Commands.Management;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;

namespace PSSharp.PowerShell.RabbitMQ.Commands;

[Cmdlet(
    VerbsLifecycle.Register,
    Nouns.RabbitMqQueue,
    SupportsShouldProcess = true,
    RemotingCapability = RemotingCapability.OwnedByCommand,
    ConfirmImpact = ConfirmImpact.Low,
    DefaultParameterSetName = AmqpParameterSet
)]
[OutputType(typeof(QueueDeclareOk), ParameterSetName = [AmqpParameterSet])]
[OutputType(typeof(QueueDetails), ParameterSetName = [ManagementParameterSet])]
public sealed class RegisterRabbitMqQueueCommand : RmqCmdlet, IDynamicParameters
{
    private const string AmqpParameterSet = "Amqp";
    private const string ManagementParameterSet = "Management";

    /// <summary>
    /// According to the AMQP specification, if the queue name is empty a queue name will be generated
    /// by the server.
    /// </summary>
    [Parameter(Mandatory = true, Position = 0, ValueFromPipelineByPropertyName = true)]
    [Alias("QueueName", "Name")]
    [AllowEmptyString]
    [QueueNameCompleter]
    public string Queue { get; set; } = string.Empty;

    [Parameter(ValueFromPipelineByPropertyName = true)]
    public SwitchParameter Durable { get; set; }

    [Parameter(ValueFromPipelineByPropertyName = true, ParameterSetName = AmqpParameterSet)]
    public SwitchParameter Exclusive { get; set; }

    [Parameter(ValueFromPipelineByPropertyName = true)]
    public SwitchParameter AutoDelete { get; set; }

    [Parameter(ValueFromPipelineByPropertyName = true)]
    public Hashtable? Arguments { get; set; }

    [Parameter(Mandatory = true, ParameterSetName = AmqpParameterSet)]
    [RabbitConnectionCompleter]
    [RabbitConnection]
    public PSRabbitConnection? Connection { get; set; }

    [Parameter(Mandatory = true, ParameterSetName = ManagementParameterSet)]
    [ManagementServerUriCompleter]
    [ValidateAbsoluteUri]
    [ValidateUriScheme("http", "https")]
    public Uri? ManagementUri { get; set; }

    [Parameter(ParameterSetName = ManagementParameterSet, ValueFromPipelineByPropertyName = true)]
    [Alias("VirtualHost")]
    [ValidateNotNullOrEmpty]
    [VirtualHostCompleter]
    public string VHost { get; set; } = "/";

    [Parameter(ParameterSetName = ManagementParameterSet)]
    [Credential]
    [ArgumentCompletions("guest")]
    public PSCredential? Credential { get; set; }

    [Parameter]
    public SwitchParameter PassThru { get; set; }

    private readonly TypedArguments _typedArguments = new();

    public object? GetDynamicParameters()
    {
        return Arguments is null ? _typedArguments : null;
    }

    private Dictionary<string, object?> ArgumentsDictionary()
    {
        if (Arguments is null)
        {
            return _typedArguments.ToDictionary();
        }
        var d = new Dictionary<string, object?>();
        foreach (DictionaryEntry entry in Arguments ?? [])
        {
            var key = LanguagePrimitives.ConvertTo<string>(entry.Key);
            d[key] = entry.Value;
        }
        return d;
    }

    protected override async Task ProcessRecordAsync()
    {
        switch (ParameterSetName)
        {
            case AmqpParameterSet:
                await Amqp();
                break;
            case ManagementParameterSet:
                await Management();
                break;
            default:
                throw new UnreachableException("Parameter set is not implemented.");
        }
    }

    private async Task Amqp()
    {
        Debug.Assert(
            Connection is not null,
            "Parameter 'Connection' is mandatory for Amqp parameter set."
        );

        var channel = await Connection.GetDefaultChannel(PipelineStopToken);

        if (!await InvokeAsync(() => ShouldProcess(Queue, "declare queue")))
        {
            return;
        }

        QueueDeclareOk declaration;
        try
        {
            declaration = await channel.QueueDeclareAsync(
                Queue,
                Durable,
                Exclusive,
                AutoDelete,
                ArgumentsDictionary(),
                false,
                noWait: false,
                PipelineStopToken
            );
        }
        catch (OperationInterruptedException ex)
            when (ex.ShutdownReason is { ReplyCode: Constants.PreconditionFailed })
        {
            var er = new ErrorRecord(ex, "PreconditionFailed", ErrorCategory.InvalidArgument, Queue)
            {
                ErrorDetails = new(
                    "One or more arguments provided to declare the queue do not match the preexisting arguments "
                        + "of the queue. Queue arguments cannot be modified once the queue has been created. "
                        + ex.ShutdownReason.ReplyText
                )
                {
                    RecommendedAction =
                        "Do not modify a queue using this cmdlet. Either remove and recreate the queue; or use "
                        + "a policy to manage the queue; or use this cmdlet with the same parameters so that the "
                        + "queue is not modified.",
                },
            };
            await WriteErrorAsync(er);
            return;
        }

        if (PassThru)
        {
            await WriteObjectAsync(declaration);
        }
        else if (Queue == string.Empty)
        {
            await InvokeAsync(() =>
                WriteWarning(
                    $"Auto-named queues cannot be used unless the queue name is captured. Specify the -PassThru parameter to capture the queue name. The auto-generated queue name is \"{declaration.QueueName}\"."
                )
            );
        }
    }

    private async Task Management()
    {
        Debug.Assert(
            ManagementUri is not null,
            "Parameter 'ManagementUri' is mandatory for Management parameter set."
        );

        var cred = Credential?.GetNetworkCredential() ?? ManagementUri.GetNetworkCredential();
        if (cred is null)
        {
            await this.ManagementCredentialRequiredError(ManagementUri);
            return;
        }

        using var client = new ManagementClient(ManagementUri.WithoutCredential(), cred);

        if (!await InvokeAsync(() => ShouldProcess(Queue, "declare queue")))
        {
            return;
        }

        // Management API does not support broker-generated queue names so to provide a seamless integration
        // we can use client-generated names. There's no behavioral difference to emulate, just the name.
        var queueName = string.IsNullOrEmpty(Queue) ? $"powershell.gen-{Guid.NewGuid():N}" : Queue;

        await client.DeclareQueue(
            VHost,
            queueName,
            new()
            {
                Arguments = ArgumentsDictionary(),
                AutoDelete = AutoDelete,
                Durable = Durable,
            },
            PipelineStopToken
        );

        if (PassThru)
        {
            var response = await client.GetQueue(VHost, queueName, PipelineStopToken);
            await WriteObjectAsync(response);
        }
    }

    public sealed class TypedArguments
    {
        [Parameter]
        [ValidateSet("classic", "quorum", "stream")]
        public string? QueueType { get; set; }

        [Parameter]
        [ValidateRange("00:00:00", "24.20:31:23.647")]
        public TimeSpan? MessageTimeToLive { get; set; } // milliseconds

        [Parameter]
        [ValidateRange("00:00:00.001", "24.20:31:23.647")]
        public TimeSpan? Expires { get; set; } // milliseconds

        [Parameter]
        [ValidateRange(0, int.MaxValue)]
        public int? MaxLength { get; set; }

        [Parameter]
        [ValidateRange(0, int.MaxValue)]
        public int? MaxLengthBytes { get; set; }

        [Parameter]
        [ValidateSet("drop-head", "reject-publish", "reject-publish-dlx")]
        public string? Overflow { get; set; }

        [Parameter]
        public string? DeadLetterExchange { get; set; }

        [Parameter]
        public string? DeadLetterRoutingKey { get; set; }

        [Parameter]
        public SwitchParameter SingleActiveConsumer { get; set; }

        /// <remarks>
        /// Quorum queues ignore this and always support piorities 0-31.
        /// </remarks>
        [Parameter]
        public byte? MaxPriority { get; set; }

        [Parameter]
        [ValidateSet("lazy", "default")]
        public string? QueueMode { get; set; }

        public Dictionary<string, object?> ToDictionary()
        {
            var d = new Dictionary<string, object?>();
            if (QueueType is not null)
            {
                d.Add("x-queue-type", QueueType);
            }
            if (MessageTimeToLive is not null)
            {
                d.Add("x-message-ttl", (uint)MessageTimeToLive.Value.TotalMilliseconds);
            }
            if (Expires is not null)
            {
                d.Add("x-expires", (uint)Expires.Value.TotalMilliseconds);
            }
            if (MaxLength is not null)
            {
                d.Add("x-max-length", MaxLength.Value);
            }
            if (MaxLengthBytes is not null)
            {
                d.Add("x-max-length-bytes", MaxLengthBytes.Value);
            }
            if (Overflow is not null)
            {
                d.Add("x-overflow", Overflow);
            }
            if (DeadLetterExchange is not null)
            {
                d.Add("x-dead-letter-exchange", DeadLetterExchange);
            }
            if (DeadLetterRoutingKey is not null)
            {
                d.Add("x-dead-letter-routing-key", DeadLetterRoutingKey);
            }
            if (SingleActiveConsumer)
            {
                d.Add("x-single-active-consumer", "true");
            }
            if (MaxPriority is not null)
            {
                d.Add("x-max-priority", MaxPriority.Value);
            }
            if (QueueMode is not null)
            {
                d.Add("x-queue-mode", QueueMode);
            }

            return d;
        }
    }
}
