using Aluna.Abstractions;

namespace Aluna.EventStore;

public interface IEventFactFactory
{
    EventFact CreateEventFact(EventRecord record);
}
