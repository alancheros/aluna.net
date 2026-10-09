namespace Aluna.Abstractions;

/// <summary>
/// Holds the aggregate identifier and its event stream name.
/// </summary>
public record AggregateStreamKey
{
    public string StreamName { get; init; }
    public Guid AggregateId { get; init; }

    public AggregateStreamKey(string streamName, Guid aggregateId)
    {
        StreamName = streamName ?? throw new ArgumentNullException(nameof(streamName));
        if (aggregateId == Guid.Empty)
        {
            throw new ArgumentException("StreamKey cannot be empty.", nameof(aggregateId));
        }
        else
        {
            AggregateId = aggregateId;
        }
    }

    private AggregateStreamKey()
    {
        StreamName = "NullObject";
        AggregateId = Guid.Empty;
    }

    public AggregateStreamKey(string streamName) : this(streamName, Guid.Empty) { }

    private static readonly AggregateStreamKey _nullObject = new();
    public static AggregateStreamKey NullObject { get => _nullObject; }

    public static implicit operator Guid(AggregateStreamKey streamKey) => streamKey.AggregateId;
}