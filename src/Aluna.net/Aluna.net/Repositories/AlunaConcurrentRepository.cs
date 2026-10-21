using Aluna.Abstractions;

namespace Aluna.Repositories;

public class AlunaConcurrentRepository<TAggregate> : IRepository<TAggregate> where TAggregate : AggregateRoot
{
    private readonly AlunaRepository<TAggregate> _innerRepository;
    private readonly SemaphoreSlim _saveAndRehydrateLock = new(1, 1);

    protected InMemoryAggregateStore<TAggregate> aggregates => _innerRepository.aggregates;

    public string StreamName => _innerRepository.StreamName;

    public AlunaConcurrentRepository(AlunaRepository<TAggregate> innerRepository)
    {
        _innerRepository = innerRepository ?? throw new ArgumentNullException(nameof(innerRepository));
    }

    public void Attach(AggregateRoot aggregate)
    {
        _innerRepository.Attach(aggregate);
    }

    public TAggregate GetById(Guid id)
    {
        return _innerRepository.GetById(id);
    }

    public TAggregate MakeNew()
    {
        return _innerRepository.MakeNew();
    }

    public void RefreshMemoryCache()
    {
        _innerRepository.RefreshMemoryCache();
    }

    public HydrationSummary RehydrateFromStoreIndex(long storeIndex)
    {
        _saveAndRehydrateLock.Wait();
        try
        {
            return _innerRepository.RehydrateFromStoreIndex(storeIndex);
        }
        finally
        {
            _saveAndRehydrateLock.Release();
        }
    }

    public Task<HydrationSummary> RehydrateFromStoreIndexAsync(long storeIndex, CancellationToken cancellationToken = default)
    {
        return RehydrateWithLockAsync(storeIndex, cancellationToken);
    }

    public void Save(AggregateRoot aggregate, long expectedAggregateSequence)
    {
        _saveAndRehydrateLock.Wait();
        try
        {
            _innerRepository.Save(aggregate, expectedAggregateSequence);
        }
        finally
        {
            _saveAndRehydrateLock.Release();
        }
    }

    public Task WaitForRehydrationWorkersAsync(CancellationToken cancellationToken = default)
    {
        return _innerRepository.WaitForRehydrationWorkersAsync(cancellationToken);
    }


    private async Task<HydrationSummary> RehydrateWithLockAsync(long storeIndex, CancellationToken cancellationToken)
    {
        await _saveAndRehydrateLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            return await _innerRepository.RehydrateFromStoreIndexAsync(storeIndex, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _saveAndRehydrateLock.Release();
        }
    }

    public void SetCreateInstanceFunc(Func<TAggregate> createInstanceFunc) => _innerRepository.SetCreateInstanceFunc(createInstanceFunc);

    public long GetFirstIndexOfDate(DateTime dateTime)
    {
        return _innerRepository.GetFirstIndexOfDate(dateTime);
    }
}
