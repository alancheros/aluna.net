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
    public AggregateStreamKey StreamKey { get; set; }

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
    /// Clears any uncommitted events and resets the aggregate to the state represented by the provided events.
    /// </summary>
    public abstract void RebuildFromHistory(IEnumerable<EventFact> events);


    /// <summary>
    /// Appends new events to the aggregate as a backend resfresh operation. This method is intended for use in scenarios where the aggregate needs to be updated with new events from an external source, such as a message bus or event store.
    /// </summary>
    /// <param name="events"></param>
    public abstract void RefreshWithEvents(IEnumerable<EventFact> events);


    public SequenceInfo SequenceIndices { get; protected internal set; } = new SequenceInfo() { StoreIndex = -1 };

    protected AggregateRoot(int contractVersion)
    {
        if (contractVersion <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(contractVersion), "Contract version must be a non-negative integer.");
        }
        ContractVersion = contractVersion;
        StreamKey = AggregateStreamKey.NullObject;
    }

    public class SequenceInfo
    {
        private int aggregateIndex = -1;

        public long StoreIndex { get; internal set; }
        public int AggregateIndex { get => aggregateIndex; }

        public int IncrementAggregateIndex(int incrementBy) => Interlocked.Add(ref aggregateIndex, incrementBy);

        public void Clear()
        {
            StoreIndex = -1;
            aggregateIndex = -1;
        }
    }
}