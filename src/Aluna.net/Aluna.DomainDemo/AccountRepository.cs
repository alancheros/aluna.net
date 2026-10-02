using Aluna.EventStore;
using Aluna.Repositories;

namespace Aluna.DomainDemo;

public class AccountRepository : AlunaRepository<Account>
{

    protected override Account CreateInstance()
    {
        return new Account(1);
    }

    public AccountRepository(IAlunaEventStore eventStore) : base(eventStore)
    {
    }
}
