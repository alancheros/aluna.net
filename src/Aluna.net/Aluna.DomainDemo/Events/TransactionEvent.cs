using System.Reflection;
using Aluna.DomainDemo.Commands;

namespace Aluna.DomainDemo.Events;
public class TransactionEvent : Abstractions.EventFact
{
    public decimal Amount { get; internal set; }

    public TransactionEvent(TransactionCommand command)
    {
        Amount = command.Amount;
    }

    public TransactionEvent(decimal amount)
    {
         Amount = amount;
    }
}
