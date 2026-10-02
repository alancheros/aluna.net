using Aluna.Abstractions;
using Aluna.DomainDemo;
using Aluna.EventStore;
using Microsoft.Extensions.Logging;
using NSubstitute;

namespace Aluna.test.Abstractions;

public abstract class AccountTestBase : EventSourceTestBase<Account>
{
    protected override ICommandHandler CommandHandler { get; }

    protected AccountTestBase()
    {
        eventStore = new InMemoryEventStore();
        repository = new AccountRepository(eventStore);
        CommandHandler = new AccountCommandHandler(Substitute.For<ILogger<AccountCommandHandler>>(), repository);
    }
}