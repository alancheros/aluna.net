namespace Aluna.Abstractions;

public interface ICommandHandler
{
    CommandResult Handle(Command command);
}
