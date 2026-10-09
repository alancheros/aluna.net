using Aluna.Abstractions;
namespace Aluna.EventStore;

public class InMemoryEventStore : IAlunaEventStore
{
    private long _storeSequence = 0;
    private readonly Dictionary<string, Dictionary<Guid, List<StoredAggregateEvent>>> _streams = new();

    private readonly Dictionary<Guid, long> eventSequence = new();
    private readonly Lock _sync = new();

    public AppendResult AppendEvents(AggregateStreamKey key, IEnumerable<EventFact> events, long expectedAggregateSequence = -1)
    {
        ArgumentNullException.ThrowIfNull(events);
        ArgumentNullException.ThrowIfNull(key);

        if (string.IsNullOrWhiteSpace(key.StreamName))
        {
            throw new ArgumentException("Stream name is required.", nameof(key));
        }

        lock (_sync)
        {
            List<StoredAggregateEvent> streamEvents = GetOrCreateStreamEvents(key);

            var currentLastId = streamEvents.Count - 1;
            if (expectedAggregateSequence >= 0 && expectedAggregateSequence != currentLastId)
            {
                throw new InvalidOperationException($"Concurrency conflict on stream '{key}'. Expected last id {expectedAggregateSequence}, actual {currentLastId}.");
            }

            foreach (var sourceEvent in events)
            {
                _storeSequence++;
                var storedEvent = new StoredAggregateEvent(sourceEvent, key.AggregateId, _storeSequence, currentLastId + 1);
                streamEvents.Add(storedEvent);
                currentLastId = storedEvent.AggregateSequence;
                eventSequence.Add(storedEvent.EventId, storedEvent.StoreSequence);
            }

            return new AppendResult(_storeSequence, events.Count(), true, string.Empty);
        }
    }

    private List<StoredAggregateEvent> GetOrCreateStreamEvents(AggregateStreamKey key)
    {
        if (!_streams.TryGetValue(key.StreamName, out var stream))
        {
            stream = [];
            _streams[key.StreamName] = stream;
        }

        if (!stream.TryGetValue(key.AggregateId, out var aggregateEvents))
        {
            aggregateEvents = [];
            stream[key.AggregateId] = aggregateEvents;
        }

        return aggregateEvents;
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
            if (!_streams.TryGetValue(streamId.StreamName, out var aggregateStreams)
                || !aggregateStreams.TryGetValue(streamId.AggregateId, out var events)
                || events.Count == 0)
                return Array.Empty<EventFact>();

            return events
                .OrderBy(x => x.AggregateSequence)
                .Select(x => x.Event)
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
            if (!_streams.TryGetValue(streamName, out var aggregateStreams) || aggregateStreams.Count == 0)
                return Array.Empty<EventFact>();

            var events = aggregateStreams.Values.SelectMany(x => x);

            return events
                .Where(x => x.StoreSequence >= fromEventId)
                .OrderBy(x => x.StoreSequence)
                .Take(maxCount)
                .Cast<EventFact>()
                .ToArray();
        }
    }

    private sealed class StoredAggregateEvent : EventFact, IStoredAggregateEvent
    {
        public Guid EventId { get; set; } = Guid.NewGuid();
        public Guid AggregateId { get; }

        public EventFact Event { get; }

        public override string EventMessage => eventMessage;

        private readonly string eventMessage;

        public StoredAggregateEvent(EventFact source, Guid aggregateId, long eventId, int aggregateSequence)
        {
            Event = source;
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
