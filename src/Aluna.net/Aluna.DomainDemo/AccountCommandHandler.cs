using Aluna.Abstractions;
using Aluna.DomainDemo.Commands;
using Aluna.DomainDemo.Events;
using Aluna.Repositories;
using Microsoft.Extensions.Logging;

namespace Aluna.DomainDemo
{
    public class AccountCommandHandler : ICommandHandler
    {
        private readonly ILogger<AccountCommandHandler> _logger;
        private readonly IRepository<Account> _repository;

        public CommandResult Handle(Command command)
        {
            switch (command)
            {
                case CreateAccountCommand createNewJob:
                    Handle(createNewJob);
                    break;
                default:
                    throw new EventSourcingException($"No handler for command type {command.GetType().Name}");
            }
            return CommandResult.Success;
        }

        private void Handle(CreateAccountCommand command)
        {

            var item = _repository.MakeNew();
            item.ApplyEvent(new AccountCreatedEvent(command));
            _repository.Save(item, command.ExpectedId);
            _repository.RefreshMemoryCache();
        }

        public AccountCommandHandler(ILogger<AccountCommandHandler> logger, IRepository<Account> repository)
        {
            _logger = logger;
            _repository = repository;
        }
    }
}
