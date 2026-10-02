using Aluna.Abstractions;
using Aluna.DomainDemo.Events;

namespace Aluna.DomainDemo;

public partial class Account
{
    public bool ApplyEvent(EventFact exporterEvent)
    {
        ArgumentNullException.ThrowIfNull(exporterEvent);
        bool wasApplied = false;
        try
        {
            var nextStatus = GetNextStatus(exporterEvent);
            wasApplied = exporterEvent switch
            {
                AccountCreatedEvent e => ApplyEvent(e),
                TransactionEvent e => ApplyEvent(e),
                _ => throw new EventSourcingException($"Event type '{exporterEvent.GetType().Name}' is not supported in class {nameof(Account)}.")
            };
            if (wasApplied)
            {
                SetLastEvent(exporterEvent);
                Status = nextStatus;
            }
            return wasApplied;
        }
        finally
        {
            if (wasApplied)
            {
                AddToUncommited(exporterEvent);
            }
        }
    }

    private bool ApplyEvent(AccountCreatedEvent e)
    {
        Id = new AggregateStreamAndId(STREAM_NAME, e.Id);
        CreatedAtUtc = DateTimeOffset.UtcNow;
        return true;
    }

    private bool ApplyEvent(TransactionEvent e)
    {
        return true;
    }
}
