namespace Aluna.Abstractions;

/// <summary>
/// Base type for aggregate roots in the domain model.
/// </summary>
public abstract class AggregateRoot : IDomainObject
{
    /// <summary>
    /// Gets the contract version used by this aggregate.
    /// </summary>
    public int ContractVersion { get; init; }

    /// <summary>
    /// Gets or sets the aggregate identifier and stream information.
    /// </summary>
    public AggregateStreamAndId Id { get; set; }

    /// <summary>
    /// Returns the events that have not been committed yet.
    /// </summary>
    public abstract IEnumerable<EventFact> GetUncommittedEvents();

    /// <summary>
    /// Marks the specified number of pending events as committed.
    /// </summary>
    public abstract void MarkEventsAsCommitted(int committedCount);

    /// <summary>
    /// Rebuilds the aggregate state from historical events.
    /// </summary>
    public abstract void LoadFromHistory(IEnumerable<EventFact> events);

    protected AggregateRoot(int contractVersion)
    {
        if (contractVersion <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(contractVersion), "Contract version must be a non-negative integer.");
        }
        ContractVersion = contractVersion;
        Id = AggregateStreamAndId.NullObject;
    }
}