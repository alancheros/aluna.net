namespace Aluna.Abstractions;

/// <summary>
/// Holds the aggregate identifier and its event stream name.
/// </summary>
public record AggregateStreamAndId
{
    public string Name { get; init; }
    public Guid AggregateId { get; init; }

    public AggregateStreamAndId(string name, Guid aggregateId)
    {
        Name = name ?? throw new ArgumentNullException(nameof(name));
        if (aggregateId == Guid.Empty)
        {
            throw new ArgumentException("Id cannot be empty.", nameof(aggregateId));
        }
        else
        {
            AggregateId = aggregateId;
        }
    }

    private AggregateStreamAndId()
    {
        Name = "NullObject";
        AggregateId = Guid.Empty;
    }

    public AggregateStreamAndId(string name) : this(name, Guid.Empty) { }

    private static readonly AggregateStreamAndId _nullObject = new();
    public static AggregateStreamAndId NullObject { get => _nullObject; }

    public static implicit operator Guid(AggregateStreamAndId streamAndId) => streamAndId.AggregateId;
}