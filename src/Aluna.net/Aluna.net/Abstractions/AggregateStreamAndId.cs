namespace Aluna.Abstractions;

/// <summary>
/// Holds the aggregate identifier and its event stream name.
/// </summary>
public class AggregateStreamAndId
{
    /// <summary>
    /// Gets or sets the unique aggregate identifier.
    /// </summary>
    public Guid AggregateId { get; set; } = Guid.Empty;

    /// <summary>
    /// Gets or sets the aggregate stream name.
    /// </summary>
    public string AggregateStream { get; set; } = string.Empty;

    /// <summary>
    /// Represents an empty aggregate identifier object.
    /// </summary>
    public static AggregateStreamAndId NullObject { get; } = new AggregateStreamAndId();
}