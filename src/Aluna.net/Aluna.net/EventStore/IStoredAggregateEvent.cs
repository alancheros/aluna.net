using Aluna.Abstractions;

namespace Aluna.EventStore;

public interface IStoredAggregateEvent
{
    Guid AggregateId { get; }
    EventFact Event { get; }
}
