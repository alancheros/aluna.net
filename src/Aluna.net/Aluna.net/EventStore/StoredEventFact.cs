using Aluna.Abstractions;

namespace Aluna.EventStore;

public partial class SqlServerEventStore
{
    private sealed class StoredEventFact : EventFact
    {
        private readonly string eventMessage;
        private readonly string readedEventType;
        public StoredEventFact(long storeSequence, int aggregateSequence, string eventType, DateTimeOffset eventTimestamp, Guid eventTraceId, string eventMessage, string? userId)
        {
            StoreSequence = storeSequence;
            AggregateSequence = aggregateSequence;
            readedEventType = eventType;
            EventTimestamp = eventTimestamp;
            CorrelationId = eventTraceId;
            this.eventMessage = eventMessage;
            UserId = userId ?? string.Empty;
        }

        public override string EventMessage => eventMessage;

        public override string EventType => readedEventType;
    }
}