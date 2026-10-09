using Aluna.Abstractions;

namespace Aluna.EventStore;

/// <summary>
/// Provides append/read operations for event streams used by the document exporter.
/// </summary>
public partial interface IAlunaEventStore
{
    /// <summary>
    /// Appends one or more events to the specified stream.
    /// </summary>
    /// <param name="key">The aggregate stream key.</param>
    /// <param name="events">The events to append, in write order.</param>
    /// <param name="expectedAggregateSequence">
    /// The expected last event id in the stream for optimistic concurrency checks.
    /// Use <c>-1</c> to skip concurrency validation.
    /// </param>
    /// <returns>
    /// The last persisted event id after the append operation completes.
    /// </returns>
    AppendResult AppendEvents(AggregateStreamKey key, IEnumerable<EventFact> events, long expectedAggregateSequence = -1);

    /// <summary>
    /// Reads events from the specified stream.
    /// </summary>
    /// <param name="streamName">The logical stream name to read from.</param>
    /// <param name="fromStoreSequence">The starting event id (inclusive). Default is <c>0</c>.</param>
    /// <param name="maxCount">The maximum number of events to return. Default is <c>100</c>.</param>
    /// <returns>
    /// A sequence of events from the stream, ordered by event id ascending.
    /// </returns>
    IEnumerable<EventFact> ReadEvents(string streamName, long fromStoreSequence = 0, int maxCount = 100);

    /// <summary>
    /// Gets all events for the specified aggregate stream and id.
    /// </summary>
    /// <param name="streamId">The aggregate stream and id to get events for.</param>
    /// <returns>A sequence of events for the specified aggregate.</returns>
    IEnumerable<EventFact> GetEventsForAggregate(AggregateStreamKey streamId);

    /// <summary>
    /// Gets the sequence id of the specified event in the event store.
    /// </summary>
    /// <param name="eventId">The unique identifier of the event.</param>
    /// <returns>The sequence id of the event.</returns>
    long GetSequenceByEventId(Guid eventId);
}
