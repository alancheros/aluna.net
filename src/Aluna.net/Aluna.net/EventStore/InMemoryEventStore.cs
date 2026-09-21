using Aluna.Abstractions;
namespace Aluna.EventStore;

public class InMemoryEventStore : IAlunaEventStore
{
    private readonly Dictionary<string, List<EventFact>> _streams = new();
    private readonly object _sync = new();

    public AppendResult AppendEvents(AggregateStreamAndId streamId, IEnumerable<EventFact> events, long expectedId = -1)
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
                streamEvents = new List<EventFact>();
                _streams[streamId.Name] = streamEvents;
            }

            var currentLastId = streamEvents.Count == 0 ? -1L : streamEvents[^1].EventId;
            if (expectedId >= 0 && expectedId != currentLastId)
            {
                throw new InvalidOperationException(
                    $"Concurrency conflict on stream '{streamId}'. Expected last id {expectedId}, actual {currentLastId}.");
            }

            foreach (var sourceEvent in events)
            {
                var nextId = currentLastId + 1;
                var storedEvent = new StoredDocumentExporterEvent(sourceEvent, nextId);
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
                .Where(x => x.EventId >= 0)
                .OrderBy(x => x.EventId)
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
                .ToArray();
        }
    }

    private sealed class StoredDocumentExporterEvent : EventFact
    {
        private readonly string eventMessage;

        public override string EventMessage => eventMessage;

        public StoredDocumentExporterEvent(EventFact source, long eventId)
        {
            EventId = eventId;
            EventType = source.EventType;
            EventTimestamp = source.EventTimestamp;
            CorrelationId = source.CorrelationId;
            eventMessage = source.EventMessage;
            UserId = source.UserId;
        }

    }
}
