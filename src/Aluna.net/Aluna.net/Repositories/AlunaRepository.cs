using Aluna.Abstractions;
using Aluna.EventStore;
using Aluna.Exceptions;

namespace Aluna.Repositories;

public class AlunaRepository<T> : IRepository<T> where T : AggregateRoot
{
    private readonly IAlunaEventStore _eventStore;
    private long lastEventId = long.MinValue;

    public virtual string StreamName => typeof(T).Name; //The stream name is the name of the aggregate type by default. Override this in derived classes if you want a different stream name.

    private readonly InMemoryAggregateStore<T> aggregates = new();
    private readonly List<T> createdAggregates = new();

    protected virtual T CreateInstance()
        => throw new NotImplementedException("CreateInstance must be implemented in derived classes.");

    public T MakeNew()
    {
        var result = CreateInstance();
        createdAggregates.Add(result);
        return result;
    }

    public void Save(AggregateRoot aggregate, long expectedId)
    {
        ArgumentNullException.ThrowIfNull(aggregate);
        if (expectedId == long.MinValue) { expectedId = lastEventId; }
        var appendedResult = _eventStore.AppendEvents(aggregate.Id, aggregate.GetUncommittedEvents(), expectedId);
        if (appendedResult.Success)
        {
            aggregate.MarkEventsAsCommitted(appendedResult.AppendedCount);
            lastEventId = appendedResult.LastEventId;
        }
    }

    public T GetById(Guid id)
    {
        if (aggregates.TryGet(id, out var memAggregate))
        {
            return memAggregate ?? throw new EventSourcingException("Should never be thrown");
        }
        var events = _eventStore.GetEventsForAggregate(new(StreamName, id));

        if (events == null || !events.Any())
        {
            throw new AggregateNotFoundException($"Aggregate with ID {id} not found.");
        }

        var aggregate = MakeNew();
        aggregate.LoadFromHistory(events);
        return aggregate;
    }

    public void RefreshMemoryCache()
    {
        List<int> movedIndexes = [];
        for (int i = 0; i < createdAggregates.Count; i++)
        {
            var aggregate = createdAggregates[i];
            if (!aggregates.Contains(aggregate.Id.AggregateId))
            {
                aggregates.Add(aggregate.Id.AggregateId, aggregate);
                movedIndexes.Add(i);
            }
        }
        for (int i = movedIndexes.Count - 1; i >= 0; i--)
        {
            createdAggregates.RemoveAt(movedIndexes[i]);
        }
    }

    public void Attach(AggregateRoot aggregate) => aggregates.Add(aggregate.Id.AggregateId, (T)aggregate);

    public AlunaRepository(IAlunaEventStore eventStore)
    {
        _eventStore = eventStore ?? throw new ArgumentNullException(nameof(eventStore));
    }
}
