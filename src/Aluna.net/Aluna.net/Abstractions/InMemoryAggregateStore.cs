using System.Diagnostics.CodeAnalysis;

namespace Aluna.Abstractions;

/// <summary>
/// Thread-safe in-memory cache for aggregate instances indexed by aggregate identifier.
/// </summary>
public sealed class InMemoryAggregateStore<TAggregate> where TAggregate : AggregateRoot
{
    // O(1) lookup by aggregate id.
    private readonly Dictionary<Guid, TAggregate> _byId = new();

    private readonly Lock _gate = new();

    /// <summary>
    /// Tries to get an aggregate by identifier.
    /// </summary>
    public bool TryGet(Guid id, out TAggregate? aggregate)
    {
        lock (_gate)
        {
            return _byId.TryGetValue(id, out aggregate);
        }
    }

    /// <summary>
    /// Adds an aggregate if absent; otherwise returns the existing instance.
    /// </summary>
    public TAggregate Add(Guid id, TAggregate aggregate)
    {
        lock (_gate)
        {
            if (_byId.TryGetValue(id, out var existing))
                return existing;

            _byId[id] = aggregate;
            return aggregate;
        }
    }

    /// <summary>
    /// Inserts or replaces an aggregate by its identifier.
    /// </summary>
    public void Upsert(TAggregate aggregate)
    {
        lock (_gate)
        {
            _byId[aggregate.StreamKey.AggregateId] = aggregate;
        }
    }

    /// <summary>
    /// Removes an aggregate by identifier.
    /// </summary>
    public bool Remove(Guid id)
    {
        lock (_gate)
        {
            if (!_byId.Remove(id))
                return false;

            return true;
        }
    }

    /// <summary>
    /// Gets the current aggregate count.
    /// </summary>
    public int Count
    {
        get { lock (_gate) return _byId.Count; }
    }

    /// <summary>
    /// Counts the number of aggregates that match the given predicate.
    /// </summary>
    /// <param name="predicate">The predicate to match aggregates.</param>
    /// <returns>The number of aggregates that match the predicate.</returns>
    public int CountItems(Func<TAggregate, bool> predicate)
    {
        lock (_gate)
        {
            return _byId.Values.Count(predicate);
        }
    }

    /// <summary>
    /// Gets all aggregates that match the given predicate.
    /// </summary>
    /// <param name="predicate">The predicate to match aggregates.</param>
    /// <returns>The aggregates that match the predicate.</returns>
    public IEnumerable<TAggregate> Get(Func<TAggregate, bool> predicate)
    {
        lock (_gate)
        {
            return _byId.Values.Where(predicate).ToArray();
        }
    }


    /// <summary>
    /// Checks whether an aggregate exists for the given identifier.
    /// </summary>
    public bool Contains(Guid id)
    {
        lock (_gate)
        {
            return _byId.ContainsKey(id);
        }
    }
}
