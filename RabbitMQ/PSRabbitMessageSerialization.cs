namespace PSSharp.PowerShell.RabbitMQ;

public enum PSRabbitMessageSerialization
{
    /// <summary>
    /// Use CLIXML to serialize the payload.
    /// </summary>
    Clixml,
    /// <summary>
    /// Convert using UTF-8 JSON.
    /// </summary>
    Json,
    /// <summary>
    /// Serialize directly as a string. If the value is not a string, convert it to one before serializing
    /// the message.
    /// </summary>
    Utf8,
    /// <summary>
    /// Serialize the message directly; it must be provided as binary data (byte[]) for this serialization type.
    /// If it is not, an exception will be thrown.
    /// </summary>
    Binary,
}