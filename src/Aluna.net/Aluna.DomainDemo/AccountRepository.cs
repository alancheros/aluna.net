using Aluna.EventStore;
using Aluna.Repositories;

namespace Aluna.DomainDemo;

public class AccountRepository : AlunaRepository<Account>
{
    public override string StreamName => Account.STREAM_NAME;

    public AccountRepository(IAlunaEventStore eventStore) : base(eventStore)
    {
        SetCreateInstanceFunc(()=> new Account(1));
    }
}
