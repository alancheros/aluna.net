using Aluna.EventStore;
using Aluna.Repositories;

namespace Aluna.DomainDemo;

public class AccountRepository : AlunaRepository<Account>
{
    public override string StreamName => Account.STREAM_NAME;

    protected override Account CreateInstance()
    {
        return new Account(1);
    }

    public AccountRepository(IAlunaEventStore eventStore) : base(eventStore)
    {
    }
}
