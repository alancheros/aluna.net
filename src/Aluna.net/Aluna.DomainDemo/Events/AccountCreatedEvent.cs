using Aluna.DomainDemo.Commands;

namespace Aluna.DomainDemo.Events;

public class AccountCreatedEvent : Abstractions.EventFact
{
    public Guid Id { get; internal set; }

    public AccountCreatedEvent(CreateAccountCommand command)
    {
        Id = command.AggregateId;
    }

    public AccountCreatedEvent(Guid id)
    {
        Id = id;
    }
}
