using Aluna.Abstractions;
namespace Aluna.EventStore;

public class InMemoryEventStore : IAlunaEventStore
{
    private readonly Dictionary<string, List<StoredDocumentExporterEvent>> _streams = new();
    private readonly Dictionary<Guid, long> eventSequence = new();
    private readonly Lock _sync = new();

    public AppendResult AppendEvents(AggregateStreamKey streamId, IEnumerable<EventFact> events, long expectedAggregateSequence = -1)
    {
        ArgumentNullException.ThrowIfNull(events);
        ArgumentNullException.ThrowIfNull(streamId);

        if (string.IsNullOrWhiteSpace(streamId.StreamName))
        {
            throw new ArgumentException("Stream name is required.", nameof(streamId));
        }

        lock (_sync)
        {
            if (!_streams.TryGetValue(streamId.StreamName, out var streamEvents))
            {
                streamEvents = new List<StoredDocumentExporterEvent>();
                _streams[streamId.StreamName] = streamEvents;
            }

            var currentLastId = streamEvents.Count - 1;
            if (expectedAggregateSequence >= 0 && expectedAggregateSequence != currentLastId)
            {
                throw new InvalidOperationException($"Concurrency conflict on stream '{streamId}'. Expected last id {expectedAggregateSequence}, actual {currentLastId}.");
            }

            foreach (var sourceEvent in events)
            {
                var nextId = currentLastId + 1;
                var storedEvent = new StoredDocumentExporterEvent(sourceEvent, streamId.AggregateId, nextId, nextId);
                streamEvents.Add(storedEvent);
                currentLastId = nextId;
                eventSequence.Add(storedEvent.EventId, storedEvent.StoreSequence);
            }

            return new AppendResult(currentLastId, events.Count(), true, string.Empty);
        }
    }

    public long GetSequenceByEventId(Guid eventId)
    {
        if (eventSequence.TryGetValue(eventId, out var sequence))
        {
            return sequence;
        }
        throw new EventSourcingException("The event is not in the store");
    }

    public IEnumerable<EventFact> GetEventsForAggregate(AggregateStreamKey streamId)
    {
        ArgumentNullException.ThrowIfNull(streamId);
        if (string.IsNullOrWhiteSpace(streamId.StreamName)) throw new ArgumentException("Stream name is required.", nameof(streamId));

        lock (_sync)
        {
            if (!_streams.TryGetValue(streamId.StreamName, out var events) || events.Count == 0)
                return Array.Empty<EventFact>();

            return events
                .Where(x => x.AggregateId == streamId.AggregateId)
                .OrderBy(x => x.AggregateSequence)
                .Select(x => x.DomainEvent)
                .ToArray();
        }
    }

    public IEnumerable<EventFact> ReadEvents(string streamName, long fromEventId = 0, int maxCount = 100)
    {
        if (string.IsNullOrWhiteSpace(streamName))
            throw new ArgumentException("Stream name is required.", nameof(streamName));
        if (maxCount <= 0)
            return Array.Empty<EventFact>();

        lock (_sync)
        {
            if (!_streams.TryGetValue(streamName, out var events) || events.Count == 0)
                return Array.Empty<EventFact>();

            return events
                .Where(x => x.StoreSequence >= fromEventId)
                .OrderBy(x => x.StoreSequence)
                .Take(maxCount)
                .Cast<EventFact>()
                .ToArray();
        }
    }

    private sealed class StoredDocumentExporterEvent : EventFact, IStoredAggregateEvent
    {
        public Guid EventId { get; set; } = Guid.NewGuid();
        public Guid AggregateId { get; }

        public EventFact DomainEvent { get; }

        public override string EventMessage => eventMessage;

        private readonly string eventMessage;

        public StoredDocumentExporterEvent(EventFact source, Guid aggregateId, long eventId, int aggregateSequence)
        {
            DomainEvent = source;
            AggregateId = aggregateId;
            StoreSequence = eventId;
            AggregateSequence = aggregateSequence;
            EventType = source.EventType;
            EventTimestamp = source.EventTimestamp;
            CorrelationId = source.CorrelationId;
            eventMessage = source.EventMessage;
            UserId = source.UserId;
        }

    }
}
