namespace Aluna.Abstractions;

/// <summary>
/// Defines the aggregate root contract in the domain model.
/// </summary>
public interface IDomainObject
{
    /// <summary>
    /// Gets the aggregate identifier and stream information.
    /// </summary>
    AggregateStreamKey StreamKey { get; }

    /// <summary>
    /// Applies historical events to rebuild aggregate state.
    /// </summary>
    /// <param name="events">The historical events to apply in order.</param>
    void RebuildFromHistory(IEnumerable<EventFact> events);
}
