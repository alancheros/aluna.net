namespace Aluna.Abstractions;

/// <summary>
/// Defines the aggregate root contract in the domain model.
/// </summary>
public interface IDomainObject
{
    /// <summary>
    /// Gets the aggregate identifier and stream information.
    /// </summary>
    AggregateStreamAndId Id { get; }

    /// <summary>
    /// Applies historical events to rebuild aggregate state.
    /// </summary>
    /// <param name="events">The historical events to apply in order.</param>
    void LoadFromHistory(IEnumerable<EventFact> events);
}
