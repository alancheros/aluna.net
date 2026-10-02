using System.Collections;
using System.Numerics;
using Aluna.Repositories;

namespace Aluna.Abstractions;

/// <summary>
/// Defines persistence operations for aggregate roots.
/// </summary>
public interface IRepository<out T> where T : AggregateRoot
{
    /// <summary>
    /// Gets the stream name used by this repository.
    /// </summary>
    string StreamName { get; }

    /// <summary>
    /// Persists aggregate changes using optimistic concurrency.
    /// </summary>
    void Save(AggregateRoot aggregate, long expectedId);

    /// <summary>
    /// Loads an aggregate by identifier.
    /// </summary>
    T GetById(Guid id);

    /// <summary>
    /// Creates a new aggregate instance.
    /// </summary>
    T MakeNew();

    /// <summary>
    /// Refreshes the in-memory cache.
    /// </summary>
    void RefreshMemoryCache();

    /// <summary>
    /// Attaches an aggregate to tracking without saving it.
    /// </summary>
    void Attach(AggregateRoot aggregate);

    /// <summary>
    /// Rehydrates aggregates from the repository stream using events after the provided store index.
    /// </summary>
    HydrationSummary RehydrateFromStoreIndex(long storeIndex);
}
