using Aluna.Abstractions;
using Aluna.EventStore;
using Aluna.Repositories;

namespace Aluna.test.Abstractions;

public abstract class EventSourceTestBase<T> : IAsyncLifetime where T : AggregateRoot
{
    protected T? sut;
    protected Exception? caught;

    protected IAlunaEventStore eventStore;

    protected IRepository<T> repository;
    protected abstract ICommandHandler CommandHandler { get; }
    protected virtual bool TestingCreationOfNewAggregate => false;


    public bool HasException => caught != null;
    public abstract IEnumerable<EventFact> Given();
    public virtual IEnumerable<Command> ThenGivenCommands() => [];
    public abstract Command? When();
    public CommandResult? CommandResult { get; set; }
    public virtual T CreateSut() => repository.MakeNew();

    protected void Setup()
    {
        try
        {

            if (!TestingCreationOfNewAggregate)
            {
                sut = CreateSut();
                sut.LoadFromHistory(Given());
                repository.Attach(sut);
            }
            foreach (var command in ThenGivenCommands())
            {
                CommandHandler.Handle(command);
            }
            var whenCommand = When();
            if (whenCommand != null)
            {
                CommandResult = CommandHandler.Handle(whenCommand);
            }
        }
        catch (Exception ex)
        {
            caught = ex;
        }
    }

    public Task InitializeAsync()
    {
        Setup();
        return Task.CompletedTask;
    }

    public Task DisposeAsync() => Task.CompletedTask;

    protected EventSourceTestBase()
    {
        eventStore = new InMemoryEventStore();
        repository = new AlunaRepository<T>(eventStore);
    }
}
