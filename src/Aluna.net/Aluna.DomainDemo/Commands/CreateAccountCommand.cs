using Aluna.Abstractions;

namespace Aluna.DomainDemo.Commands;

public class CreateAccountCommand : CreateNewAggregateCommand
{
    public CreateAccountCommand(Guid id, string userId)
    {
        if (id == Guid.Empty)
        {
            throw new ArgumentException("Id cannot be empty", nameof(id));
        }
        AggregateId = id;
        UserId = userId;
    }
}
