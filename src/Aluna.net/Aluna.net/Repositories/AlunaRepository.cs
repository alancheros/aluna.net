using Aluna.Abstractions;
using Aluna.EventStore;
using Aluna.Exceptions;

namespace Aluna.Repositories;

public class AlunaRepository<T> : IRepository<T> where T : AggregateRoot
{
    private const int RehydrationPageSize = 500;
    private readonly IAlunaEventStore _eventStore;

    public virtual string StreamName => typeof(T).Name; //The stream name is the name of the aggregate type by default. Override this in derived classes if you want a different stream name.

    protected readonly InMemoryAggregateStore<T> aggregates = new();
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
        if (expectedId == long.MinValue) { expectedId = aggregate.AggregateSequence; }
        var appendedResult = _eventStore.AppendEvents(aggregate.Id, aggregate.GetUncommittedEvents(), expectedId);
        if (appendedResult.Success)
        {
            aggregate.MarkEventsAsCommitted(appendedResult.AppendedCount);
            aggregate.AggregateSequence += appendedResult.AppendedCount;
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
        aggregate.RebuildFromHistory(events);
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

    public HydrationSummary RehydrateFromStoreIndex(long storeIndex)
    {

        long processedEvents = 0;
        long skippedEvents = 0;
        int hydratedAggregates = 0;
        long lastScannedStoreIndex = storeIndex + 1;

        if (storeIndex == long.MaxValue)
        {
            return new HydrationSummary(0, 0, 0, long.MaxValue);
        }

        while (true)
        {
            var readPageResult = ReadAndGroupEventsPage(lastScannedStoreIndex);

            skippedEvents += readPageResult.SkippedEvents;

            if (readPageResult.PageSize == 0)
            {
                break;
            }

            foreach (var grouped in readPageResult.GroupedEvents)
            {
                var replayEvents = grouped.Value
                    .OrderBy(x => ((EventFact)x).EventId)
                    .Select(x => x.DomainEvent)
                    .ToArray();

                try
                {
                    processedEvents += replayEvents.Length;
                    var aggregate = GetOrCreateInstance(grouped.Key);
                    aggregate.RebuildFromHistory(replayEvents);
                    Attach(aggregate);
                    hydratedAggregates++;
                }
                catch(Exception ex)
                {
                    skippedEvents += replayEvents.Length;
                }
            }

            lastScannedStoreIndex = readPageResult.LastScannedStoreIndex + 1;

            if (lastScannedStoreIndex == long.MaxValue)
            {
                break;
            }
        }
        lastScannedStoreIndex--;
        return new HydrationSummary(processedEvents, hydratedAggregates, skippedEvents, lastScannedStoreIndex);
    }

    private AggregateRoot GetOrCreateInstance(Guid key)
    {
        if (aggregates.TryGet(key, out var existingAggregate))
        {
            return existingAggregate ?? throw new EventSourcingException("Should never be thrown");
        }
        var newAggregate = CreateInstance();
        return newAggregate;
    }

    private ReadPageResult ReadAndGroupEventsPage(long nextFromEventId)
    {
        var groupedEvents = new Dictionary<Guid, List<IStoredAggregateEvent>>();
        long skippedEvents = 0;

        var readPage = _eventStore
            .ReadEvents(StreamName, nextFromEventId, RehydrationPageSize)
            .OrderBy(x => x.EventId)
            .ToArray();

        if (readPage.Length == 0)
        {
            return new ReadPageResult(readPage.Length, nextFromEventId, skippedEvents, groupedEvents);
        }

        var lastScannedStoreIndex = nextFromEventId;
        foreach (var eventFact in readPage)
        {
            lastScannedStoreIndex = eventFact.EventId;
            if (eventFact is not IStoredAggregateEvent aggregateEvent)
            {
                skippedEvents++;
                continue;
            }

            if (!groupedEvents.TryGetValue(aggregateEvent.AggregateId, out var group))
            {
                group = [];
                groupedEvents[aggregateEvent.AggregateId] = group;
            }

            group.Add(aggregateEvent);
        }

        return new ReadPageResult(readPage.Length, lastScannedStoreIndex, skippedEvents, groupedEvents);
    }

    public AlunaRepository(IAlunaEventStore eventStore)
    {
        _eventStore = eventStore ?? throw new ArgumentNullException(nameof(eventStore));
    }


    private class ReadPageResult
    {
        public int PageSize { get; init; }
        public long LastScannedStoreIndex { get; init; }
        public long SkippedEvents { get; init; }
        public Dictionary<Guid, List<IStoredAggregateEvent>> GroupedEvents { get; init; }

        public ReadPageResult(int pageSize, long lastScannedStoreIndex, long skippedEvents, Dictionary<Guid, List<IStoredAggregateEvent>> groupedEvents)
        {
            PageSize = pageSize;
            LastScannedStoreIndex = lastScannedStoreIndex;
            SkippedEvents = skippedEvents;
            GroupedEvents = groupedEvents;
        }
    }
}

