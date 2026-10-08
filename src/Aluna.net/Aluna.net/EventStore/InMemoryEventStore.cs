using Aluna.Abstractions;
namespace Aluna.EventStore;

public class InMemoryEventStore : IAlunaEventStore
{
    private readonly Dictionary<string, List<StoredDocumentExporterEvent>> _streams = new();
    private readonly Lock _sync = new();

    public AppendResult AppendEvents(AggregateStreamAndId streamId, IEnumerable<EventFact> events, long expectedAggregateSequence = -1)
    {
        ArgumentNullException.ThrowIfNull(events);
        ArgumentNullException.ThrowIfNull(streamId);

        if (string.IsNullOrWhiteSpace(streamId.Name))
        {
            throw new ArgumentException("Stream name is required.", nameof(streamId));
        }

        lock (_sync)
        {
            if (!_streams.TryGetValue(streamId.Name, out var streamEvents))
            {
                streamEvents = new List<StoredDocumentExporterEvent>();
                _streams[streamId.Name] = streamEvents;
            }

            var currentLastId = streamEvents.Count == 0 ? -1L : streamEvents[^1].EventId;
            if (expectedAggregateSequence >= 0 && expectedAggregateSequence != currentLastId)
            {
                throw new InvalidOperationException(
                    $"Concurrency conflict on stream '{streamId}'. Expected last id {expectedAggregateSequence}, actual {currentLastId}.");
            }

            foreach (var sourceEvent in events)
            {
                var nextId = currentLastId + 1;
                var storedEvent = new StoredDocumentExporterEvent(sourceEvent, streamId.AggregateId, nextId);
                streamEvents.Add(storedEvent);
                currentLastId = nextId;
            }

            return new AppendResult(currentLastId, events.Count(), true, string.Empty);
        }
    }

    public IEnumerable<EventFact> GetEventsForAggregate(AggregateStreamAndId streamId)
    {
        ArgumentNullException.ThrowIfNull(streamId);
        if (string.IsNullOrWhiteSpace(streamId.Name)) throw new ArgumentException("Stream name is required.", nameof(streamId));

        lock (_sync)
        {
            if (!_streams.TryGetValue(streamId.Name, out var events) || events.Count == 0)
                return Array.Empty<EventFact>();

            return events
                .Where(x => x.AggregateId == streamId.AggregateId)
                .OrderBy(x => x.EventId)
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
                .Where(x => x.EventId >= fromEventId)
                .OrderBy(x => x.EventId)
                .Take(maxCount)
                .Cast<EventFact>()
                .ToArray();
        }
    }

    private sealed class StoredDocumentExporterEvent : EventFact, IStoredAggregateEvent
    {
        public Guid AggregateId { get; }

        public EventFact DomainEvent { get; }

        public override string EventMessage => eventMessage;

        private readonly string eventMessage;

        public StoredDocumentExporterEvent(EventFact source, Guid aggregateId, long eventId)
        {
            DomainEvent = source;
            AggregateId = aggregateId;
            EventId = eventId;
            EventType = source.EventType;
            EventTimestamp = source.EventTimestamp;
            CorrelationId = source.CorrelationId;
            eventMessage = source.EventMessage;
            UserId = source.UserId;
        }

    }
}
