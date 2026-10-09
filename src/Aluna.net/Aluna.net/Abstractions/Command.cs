namespace Aluna.Abstractions;

/// <summary>
/// Represents a command sent to request a specific action in the system.
/// </summary>
public abstract class Command : Message
{
    /// <summary>
    /// Gets the expected event identifier used for optimistic concurrency.
    /// </summary>
    public long ExpectedAggregateSequence { get; init; } = long.MinValue;
}

/// <summary>
/// Represents a command that creates a new aggregate instance.
/// </summary>
public abstract class CreateNewAggregateCommand : Command
{
    /// <summary>
    /// Gets the identifier of the aggregate to create.
    /// </summary>
    public Guid AggregateId { get; init; } = Guid.Empty;
}
