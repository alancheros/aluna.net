using Aluna.DomainDemo.Commands;

namespace Aluna.DomainDemo.Events;

public class AccountCreatedEvent : Abstractions.EventFact
{

    public AccountCreatedEvent(CreateAccountCommand command)
    {
        AggregateId = command.AggregateId;
    }

    public AccountCreatedEvent(Guid id)
    {
        AggregateId = id;
    }
}
