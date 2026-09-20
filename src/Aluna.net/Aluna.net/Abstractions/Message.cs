namespace Aluna.Abstractions;

/// <summary>
/// Represents a message that can be sent through a messaging pipeline.
/// </summary>
public class Message
{
    /// <summary>
    /// Gets or sets the user identifier associated with the message.
    /// </summary>
    public string UserId { get; set; } = "System";
}
