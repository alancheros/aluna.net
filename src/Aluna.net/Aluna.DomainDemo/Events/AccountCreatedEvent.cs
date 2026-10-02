using Aluna.DomainDemo.Commands;

namespace Aluna.DomainDemo.Events;

internal class AccountCreatedEvent : Abstractions.EventFact
{
    public Guid Id { get; internal set; }

    public AccountCreatedEvent(CreateAccountCommand command)
    {
        Id = command.AggregateId;
    }
}
