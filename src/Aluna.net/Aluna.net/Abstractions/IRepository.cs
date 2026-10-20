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
    void Save(AggregateRoot aggregate, long expectedAggregateSequence);

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
    /// <param name="storeIndex">The store index after which to rehydrate events.</param>
    /// <returns>The hydration summary.</returns>
    HydrationSummary RehydrateFromStoreIndex(long storeIndex);

    /// <summary>
    /// Rehydrates aggregates from the repository stream using events after the provided store index asynchronously.
    /// </summary>
    /// <param name="storeIndex">The store index after which to rehydrate events.</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>A task that represents the asynchronous operation. The task result contains the hydration summary.</returns>
    Task<HydrationSummary> RehydrateFromStoreIndexAsync(long storeIndex, CancellationToken cancellationToken = default);

    /// <summary>
    /// Waits for all rehydration workers to complete their work.
    /// </summary>
    /// <returns>A task that represents the asynchronous wait operation and completes when all rehydration workers have finished.</returns>
    Task WaitForRehydrationWorkersAsync();
}
