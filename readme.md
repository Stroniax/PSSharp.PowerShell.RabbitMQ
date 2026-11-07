# RabbitMQ PowerShell Module

## Usage

Install, then import the module.

```pwsh
# First, install the module
PS:/> Install-Module 'PSSharp.PowerShell.RabbitMQ'
# (or, using PSResourceGet to install the module)
PS:/> Install-PSResource -Name 'PSSharp.PowerShell.RabbitMQ'

# 
PS:/> Import-Module 'PSSharp.PowerShell.RabbitMQ'
```

The module supports two connection methods. Some subset of cmdlets only work with one or the other; some of the
more core operations are supported by both. In general, the `Session` options use a native RabbitMQ client and
are more performant; the `ManagementUri` options use HTTP and are more versatile.

In both cases, a credential can be supplied inline in the URI used to connect or by passing a separate
`-Credential` parameter. If both are present, the `-Credential` parameter takes precedence.

### `RabbitMqSession`, `PSRabbitSession`, and the `-Session` Parameter

To take advantage of a RabbitMQ connection, use the `-Session` parameter. This parameter takes an instance of the
`PSRabbitSession`. You can also supply the name of a session, or the InstanceId of a session; in either case,
the argument will be resolved into the associated `Session`. It is recommended to only use the name and rely on
transformation ad-hoc at the command line, and pass a variable referencing the connection directly in scripts.
If the connection name differs from a name passed to the parameter, an error will occur during parameter binding.

To create a persistent RabbitMQ amqp connection and get a `PSRabbitSession`, use the `New-RabbitMqSession` cmdlet.

```pwsh
PS:/> New-RabbitMqSession -Uri 'amqp://guest:guest@localhost:5672/'

Name      IsOpen InstanceId
----      ------ ----------
localhost True   e18bfb83-5f5c-49b6-8337-6049d6b6084d

# Or, use named arguments instead of the URI
PS:/> New-RabbitMqSession -HostName 'localhost' -Port 5672 -VirtualHost '/' -Credential 'guest' -Name 'dev'

Name IsOpen InstanceId
---- ------ ----------
dev  True   355dbced-8738-456a-8076-07de0762cd59
```

If you forgot to capture the session into a variable, or wnat to see the current sessions, use
`Get-RabbitMqSession`.

```pwsh
PS:/> Get-RabbitMqSession

Name      IsOpen InstanceId
----      ------ ----------
dev       True   355dbced-8738-456a-8076-07de0762cd59
localhost True   e18bfb83-5f5c-49b6-8337-6049d6b6084d

PS:/> $Dev = Get-RabbitMqSession -Name 'dev'
```

#### Sending and Receiving Messages (Session)

You can now use the session to declare queues and send and receive messages. Note that sending a message may
have different behavior than expected; be sure to understand the [`-Serialization`](#serialization) parameter
if the message is not solely being used between PowerShell clients.

```pwsh
PS:/> Register-RabbitMqQueue -Name 'x' -Session $Dev -Durable -QueueType 'classic' -PassThru

QueueName MessageCount ConsumerCount
--------- ------------ -------------
x                    0             0

# -Mandatory will raise an error if the message is not routed
PS:/> Send-RabbitMqMessage -RoutingKey 'x' -Session $Dev -Payload 'Test message 1' -Mandatory

# -Mandatory will raise an error if there are no messages to receive
# -ManualAck is required if you don't want the message to be auto-ack'd and will ack/nack yourself.
# Note that you must capture the message into a variable if you will ack it yourself, or it will become stranded.
PS:/> Receive-RabbitMqMessage -Queue 'x' -Session $Dev -Mandatory -ManualAck -OutVariable TestMessage

Body                        : Test message
Exchange                    : amq.default
RoutingKey                  : x
MessageCount                : 0
BasicProperties.ContentType : application/clixml+xml
BasicProperties.Headers     : {[x, System.Byte[]], [x-dotnet-pub-seq-no, 1]}

PS:/> $TestMessage | Send-RabbitMqAck

```

##### Serialization

By default, the module will serialize a message payload to CliXml. See the `ConvertTo-CliXml` and
`ConvertFrom-CliXml` cmdlets for information about this serialization. This behavior makes message
de/serialization relatively transparent to the caller for producers/consumers that all use this module.

If the message payload will be accessed outside of PowerShell or binary size is a concern, it is recommended
to use a different serialization strategy. There are a few native serialization implementations which can be
specified using the `-Serialization` parameter of the `Send-` and `Receive-RabbitMqMessage` cmdlets:

1. CliXMl: this is the default. An object (even a string) will be serialized to CliXml and the content type
    of the message will be set to `application/clixml+xml`[1].
2. JSON: the payload will be serialized similarly to `ConvertTo-Json` using non-configurable defaults.
    The content type header of the message will be set to `application/json`[1]. If more specific configuration
    of the json serialization is needed, use `-Serialization Utf8` and add the `-ContentType 'application/json'`
    parameter.
3. Utf8: The payload will be converted to a string (similarly to `[string]$YourPayloadVariable`) and the content
    type will be set to `text/plain`[1]. This is generally the most versatile for text-based serialization methods
    as you can override the content-type header[1] after serializing the message to whatever string value you
    desire (toml, yaml, xml, etc).
4. Binary: The payload will be converted to binary (similarly to `[byte[]]$YourPayloadVariable`) and the content
    type will be set to `application/octet-stream`[1]. This is ultimately the most flexible serialization type
    to be used in advanced circumstances.

If a serialization is not specified during deserialization (eg `Receive-RabbitMqMessage`) the deserialization
strategy is to look at the `content_type` header and use its value to determine the approach to try.

If serialization fails, the message *will* still be output; however, the `Body` property will be `$null`.
The message also has a `RawBody` property which contains a copy of the binary payload as `byte[]`. An error
will be written before the message is written to the pipeline describing the deserialization failure.

[1] Note that if the content type is set using the `-ContentType` parameter, the parameter argument will
override the default. This can be used for example to specify the pre-formatted content of a payload while
allowing the user to specify whichever content type is accurate.

##### Asynchronous Consumers

To create a consumer that continues running and receiving messages as they become available, use the
`Receive-RabbitMqMessage` cmdlet as before with the `-AsJob` parameter. This will change the cmdlet from
reading a single batch of messages using the `basic.get` RabbitMQ operation to a `basic.consume` that creates
a PowerShell job which emits an output object for every message delivered.

When `-AsJob` is specified, the user can also set a `-Count` parameter to determine the message count after
which the consumer should send `basic.cancel` to terminate its subscription. The job will continue to run
until all in-flight messages have been delivered to it. To address this, the user can specify the
`-PrefetchCount` parameter. However, if the queue is consuming with auto-ack mode (that is, `-MandatoryAck`
was not specified) the `-PrefetchCount` is effectively ignored. Finally, the `-UnregisteerAfterWindow`
parameter accepts a TimeSpan. When this timespan has elapsed without a message being received, the consumer
will send `basic.cancel` and the job will end.

Below is an example of using a consumer job to asynchronously process messages in a queue one at a time.

```pwsh

PS:/> $JobParameters = @{
    Queue = 'x'
    ManualAck = $true
    AsJob = $true
    UnregisterAfterWindow  = '00:00:30'
    PrefetchCount = 1
    Connection = $Dev
}

PS:/> $Job = Receive-RabbitMqMessage @JobParameters

# Assuming a message of 'Test Message 1' is published followed by a message '123' ...
PS:/> $Job | Receive-Job -Wait -AutoRemoveJob -Debug | ForEach-Object {
    $Message = $_ # So that the catch block does not reassign the value
    try {
      Write-Host "Processing message" $Message.DeliveryTag -ForegroundColor Green
      Write-Host "Message content follows: " $Message.Body
      [int]$_.Body # an operation that might fail: cast the body to an int
    }
    catch {
      Write-Error $_
      Send-RabbitMqNack $Message -Verbose -Requeue:(!$Message.Redelivered)
      return
    }
    Send-RabbitMqAck $Message -Verbose
}

Processing message 5
Message content follows Test Message 1
Write-Error: Cannot convert value "Test Message 1" to type "System.Int32". Error: "The input string 'Test Message 1' was not in a correct format."
VERBOSE: Performing the operation "nack message with requeue=True" on target "5".
Processing message 6
Message content follows Test Message 1
Write-Error: Cannot convert value "Test Message 1" to type "System.Int32". Error: "The input string 'Test Message 1' was not in a correct format."
VERBOSE: Performing the operation "nack message with requeue=False" on target "6"
Processing message 7
Message content follows 123
123
VERBOSE: Performing the operation "ack message" on target "7".

```

Currently there is not an implementation to consume and handle a message all within a job. Instead, use the
`Start-Job` cmdlet to create an out-of-process consumer. The job must manage its own session and consumer.

#### Cleaning Up the Session

When you are finished, clean up a session using `Remove-RabbitMqSession`. This closes the connection and all
associated channels, terminating running consumers. It is a best practice to clean up the session when it is
no longer needed.

```pwsh
# Here also, you can use the name of a session which resolves to the actual session during parameter binding
# Warning! If the session doesn't exist, you'll get an error during parameter binding and the cmdlet will not run.
PS:/> Remove-RabbitMqSession -Session 'dev'
PS:/> Get-RabbitMqSession | Remove-RabbitMqSession
```

### `ManagementUri` and the Management API

All cmdlets that are able to also support using the RabbitMQ management API. This can allow the user to perform
some operations 1) without a connection-capable credential (assuming the user has a management credential);
or 2) without creating a persistent connection to the RabbitMQ broker.

There are three parameters that are generally useful to this parameter set:

1. `-ManagementUri [Uri]` accepts the URI to the management API. For example: `http://guest:guest@localhost:15672/`
    This parameter is required to use a RabbitMQ management API. The URL may include the credential; it will be
    encoded into the HTTP message as a `Basic` credential per the spec, and stripped from the URI for the HTTP
    request.
2. `-Credential [PSCredential]` accepts a credential that overrides any specified in the `ManagementUri` parameter.
    A credential is required to use the management API. It can be provided via either parameter.
3. `-VHost [string]` is required for most, but not all, of the management API implementations. The default value
    `/` is sufficient for most use cases.

Unlike with the `Session` implementations, management APIs do not require a persistent connection or session to
be used. The ManagementUri is specified each time a cmdlet is called.

For example, declaring the same queue as the `Session` example looks like the following.

```pwsh
PS:/> $ManagementUri = 'http://guest:guest@localhost:15672/'
PS:/> Register-RabbitMqQueue -Queue 'x' -Durable -QueueType 'classic' -ManagementUri $ManagementUri -PassThru

QueueName MessageCount ConsumerCount
--------- ------------ -------------
x                    1             0
```

The management API often provides more data than the session-based implementations of cmdlets; pipe the result to
`select *` to see the additional fields.

#### Sending and Receiving Messages (Management)

Because a session is not required to use the management APIs, it can be tempting to use these cmdlet
implementations from PowerShell. It is not advised to use outside of ad-hoc command line use for various reason
including permissions, reliability, and performance. However, ultimately it is up to the user.

The management APIs are intended to be used very similarly to the session APIs.

```pwsh

# -Mandatory will raise an error if the message is not routed
PS:/> Send-RabbitMqMessage -RoutingKey 'x' -ManagementUri $ManagementUri -Payload 'Test message 2' -Mandatory
# Warnings ignored in this example. A warning is emitted encouraging using the `Session` parameter.

# -Mandatory will raise an error if the queue is empty
PS:/> Receive-RabbitMqMessage -Queue x -Mandatory -ManagementUri $env:RABBITMQ_MANAGEMENT_URI

Body                        : Test message 2
Exchange                    : amq.default
RoutingKey                  : x
MessageCount                : 0
BasicProperties.ContentType : application/clixml+xml

```

Note that the management API does not allow the client to ack or nack the message. Trying to ack or nack a
message produced by the management API will cause an error. The default behavior auto-acks the message to provide
a consistent experience with the `Session` implementation; to cause the message to be returned to the queue (which
is more consistent with the management plugin's HTML user interface) use the `-AckMode` parameter.

#### `Register-RabbitMqManagementServer`

A persistent management API connection is not needed for the management implementations of the cmdlets. However,
when the user is at the command-line auto completion can save a lot of time and troubleshooting. This module is
prioritizes the user experience and suggests a reasonable default for most parameters. The auto-completion can
be set up to use your actual server's records if you register the management server. This must be done per-session
and has very little impact beyond argument completion. For this reason, it is only recommended to register a
management server for interactive use of the module. (Background scripts do not benefit from calling this cmdlet.)

The completions will offer as specific data as the system can identify, regardless of whether you are using the
session or management implementation of the cmdlet or have not yet specified either parameter. (What is meant by
this is that if the `-ManagementUri` parameter is already set, the completion results will be filtered to that
server; similarly if the `-Session` parameter is already set. If neither is set, all registered servers will be
used to identify possible completion results).

```pwsh
PS:/> Register-RabbitMqManagementServer -ManagementUri $ManagementUri
PS:/> Receive-RabbitMqMessage -Queue <#Tab#>
```

### Versioning

This product uses a loose definition of semantic versioning. APIs may change between major versions (for example,
the fields of a response) after 1.0, and should be additive-only between minor versions. The "looseness" of the
usage is to account for changing when and how errors are raised. For example, if a C# exception gets thrown and
stops the pipeline, but the exception should reasonably be raised as a non-terminating error, a minor version bump
may change the error severity and the `ErrorCategory`.

## Contributing

I'm happy to review pull requests and fix the module where it is broken. I don't personally plan to implement
every feature of the RabbitMQ client nor management API as a cmdlet for lack of time and need. My goal is to
provide an exceptionally high-quality product so there is much emphasis on user comfort beyond "the minimum"
in the implementation of the module; pull requests are expected to show the same level of intentionality.

### Implementation and Design Goals

1. Use asynchronous code (mainly because the APIs the module depends on are async; I want code to be reasonably
    efficient but I don't expect to eke out performance for a PowerShell module)
    - [x] `RmqCmdlet` defines a `virtual Task ProcessRecordAsync()` for async cmdlet processing
    - [x] `RabbitMqCompletionAttribute` defines a
        `abstract IAsyncEnumerable<ArgumentCompletionText> CompleteArgument` method for async argument completion.
2. Provide argument completion in as many places as reasonable including offerring completions from the RabbitMQ
    management API where possible, or a sensible fallback if no management API is available.
3. Where supported, implement cmdlets with both the management API and the RabbitMQ client, allowing users to
    take advantage of either with a similar experience.
    - Error specifics may deviate, but an error should be propagated or not propagated to the caller consistently
        regardless of which implementation is used. For example, publishing to an exchange that does not exist
        should produce an `ObjectNotFound` error whether implemented through `RabbitMQ.Client` or the management
        API.

### Artificial Intelligence

I could give a speech about this. Developers should learn. If you use AI to learn, that's great. I believe a
higher quality of product can be attained by handcrafting with genuine intelligence, and expect that level of
effort to be apparent in any pull requests.
