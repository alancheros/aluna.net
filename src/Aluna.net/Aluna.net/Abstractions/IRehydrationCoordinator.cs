namespace Aluna.Abstractions;

public interface IRehydrationCoordinator<TRepo, TAgg> where TRepo : IRepository<TAgg> where TAgg : AggregateRoot
{
    Task TriggerAsync();
    void AddRepository(TRepo repository);
}
