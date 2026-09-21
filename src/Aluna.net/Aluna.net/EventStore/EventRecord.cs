namespace Aluna.EventStore;

public sealed class EventRecord
{
    public long EventSequenceId { get; init; }
    public Guid AggregateId { get; init; }
    public string StreamType { get; init; } = "NOT_SET";
    public int AggregateSequence { get; init; }
    public string EventType { get; init; } = "Undefined";
    public DateTime OccurredUtc { get; init; }
    public string Payload { get; init; } = string.Empty;
    public string MessageVersion { get; init; } = "0.0";
    public Guid? CorrelationId { get; init; }
    public string? UserId { get; init; }

    public EventRecord(long eventSequenceId, Guid aggregateId, string streamType, int aggregateSequence, string eventType, DateTime occurredUtc, string payload, string messageVersion, Guid? correlationId, string? userId)
    {
        EventSequenceId = eventSequenceId;
        AggregateId = aggregateId;
        StreamType = streamType;
        AggregateSequence = aggregateSequence;
        EventType = eventType;
        OccurredUtc = occurredUtc;
        Payload = payload;
        MessageVersion = messageVersion;
        CorrelationId = correlationId;
        UserId = userId;
    }

    public EventRecord() { }
}
