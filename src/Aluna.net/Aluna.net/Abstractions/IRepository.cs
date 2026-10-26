using Aluna.Repositories;

namespace Aluna.Abstractions;

/// <summary>
/// Defines persistence operations for aggregate roots.
/// </summary>
public interface IRepository<T> where T : AggregateRoot
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
    /// Rehydrates aggregates from the repository stream asynchronously.
    /// </summary>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>A task that represents the asynchronous operation. The task result contains the hydration summary.</returns>
    internal Task<HydrationSummary> RehydrateFromStoreAsync(CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Sets the start time for rehydration.
    /// </summary>
    /// <param name="dateTimeOffset">The start time for rehydration.</param>
    void SetRehydrationStart(DateTimeOffset dateTimeOffset);

    /// <summary>
    /// Waits for all rehydration workers to complete their work.
    /// </summary>
    /// <returns>A task that represents the asynchronous wait operation and completes when all rehydration workers have finished.</returns>
    Task WaitForRehydrationWorkersAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Sets the delegate used to create instances of <typeparamref name="T"/>.
    /// </summary>
    /// <param name="createInstanceFunc">Delegate that creates and returns an instance of <typeparamref name="T"/>.</param>
    void SetCreateInstanceFunc(Func<T> createInstanceFunc);

    /// <summary>
    /// Gets the first index associated with the specified date.
    /// </summary>
    /// <param name="dateTime">The date to locate.</param>
    /// <returns>The first index associated with the specified date.</returns>
    long GetFirstIndexOfDate(DateTime dateTime);

    /// <summary>
    /// Gets a value indicating whether rehydration is currently in progress.
    /// </summary>
    bool IsRehydrationInProgress { get; }
}
