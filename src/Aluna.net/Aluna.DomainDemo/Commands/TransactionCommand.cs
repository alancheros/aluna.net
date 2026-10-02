using Aluna.Abstractions;

namespace Aluna.DomainDemo.Commands;

public class TransactionCommand: Command
{
    public Guid AccountId { get; }
    public decimal Amount { get; }

    public TransactionCommand(Guid accountId, decimal amount)
    {
        AccountId = accountId;
        Amount = amount;
    } 
}