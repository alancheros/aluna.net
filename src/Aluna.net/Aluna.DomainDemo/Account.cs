using Aluna.Abstractions;
using Aluna.DomainDemo.Events;

namespace Aluna.DomainDemo;

/// <summary>
/// Single unit of work: one export task containing multiple statements.
/// </summary>
public sealed partial class Account : AggregateRoot, IDomainObject
{
    public const string STREAM_NAME = "ACCOUNT";

    private readonly Dictionary<string, string> _extensions = [];
    private EventFact lastEvent = EventFact.NullEvent;
    private readonly Queue<EventFact> _uncommittedEvents = new();

    private static readonly IReadOnlyDictionary<(DocumentExportJobStatus From, Type EventType), DocumentExportJobStatus> Transitions
        = new Dictionary<(DocumentExportJobStatus, Type), DocumentExportJobStatus>
        {
        { (DocumentExportJobStatus.Undefined, typeof(AccountCreatedEvent)), DocumentExportJobStatus.Draft },
        { (DocumentExportJobStatus.Draft, typeof(TransactionEvent)), DocumentExportJobStatus.Draft }
        };

    public DocumentExportJobStatus Status { get; private set; } = DocumentExportJobStatus.Undefined;
    public DateTimeOffset CreatedAtUtc { get; private set; } = DateTimeOffset.MinValue;
    public string SourceSystem { get; set; } = string.Empty;

    public IReadOnlyDictionary<string, string> Extensions { get => _extensions; }
    public EventFact LastEvent => lastEvent;


    private DocumentExportJobStatus GetNextStatus(EventFact exporterEvent)
    {
        (DocumentExportJobStatus From, Type EventType) key = (Status, exporterEvent.GetType());

        if (!Transitions.TryGetValue(key, out DocumentExportJobStatus nextStatus))
        {
            throw new DomainException($"Invalid transition: {Status} + {exporterEvent.GetType().Name}");
        }

        return nextStatus;
    }

    public void SetLastEvent(EventFact exporterEvent) => lastEvent = exporterEvent;


    public override IEnumerable<EventFact> GetUncommittedEvents()
    {
        return _uncommittedEvents.ToArray(); //Return a copy to prevent external modification
    }

    public override void MarkEventsAsCommitted(int committedCount)
    {
        for (int i = 0; i < committedCount && _uncommittedEvents.Count > 0; i++)
        {
            _ = _uncommittedEvents.Dequeue();
        }
    }

    public override void LoadFromHistory(IEnumerable<EventFact> events)
    {
        ArgumentNullException.ThrowIfNull(events);

        // Reset aggregate state before replaying history
        Id = AggregateStreamAndId.NullObject;
        Status = DocumentExportJobStatus.Undefined;
        _extensions.Clear();
        lastEvent = EventFact.NullEvent;
        _uncommittedEvents.Clear();

        foreach (EventFact exporterEvent in events)
        {
            ArgumentNullException.ThrowIfNull(exporterEvent);

            if (!ApplyEvent(exporterEvent))
            {
                throw new EventSourcingException($"Failed to apply historical event '{exporterEvent.GetType().Name}'.");
            }
        }

        // Historical events are already committed; don't treat them as pending.
        _uncommittedEvents.Clear();
    }

    private void AddToUncommited(EventFact exporterEvent)
    {
        _uncommittedEvents.Enqueue(exporterEvent);
    }

    public Account(int contractVersion) : base(contractVersion)
    {
        Id = AggregateStreamAndId.NullObject;
    }
}
