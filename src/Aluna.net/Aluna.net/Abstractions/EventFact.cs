using System.Text.Json;
using System.Text.Json.Serialization;

namespace Aluna.Abstractions;

/// <summary>
/// Represents an immutable event captured from aggregate activity.
/// </summary>
public abstract class EventFact : Message
{
    /// <summary>
    /// Gets the event contract version.
    /// </summary>
    public string Version { get; init; } = "1.0";

    /// <summary>
    /// Gets a sentinel event instance representing an empty event.
    /// </summary>
    public static EventFact NullEvent { get; } = new NullDocumentExporterEvent();

    /// <summary>
    /// Gets the sequential event identifier within the event store.
    /// </summary>
    public long EventStoreSequenceId { get; init; }

    /// <summary>
    /// Gets the sequential event identifier within the aggregate.
    /// </summary>
    public int AggregateSequenceId { get; init; }

    /// <summary>
    /// Gets the event type (started, completed, failed, and so on).
    /// </summary>
    public virtual string EventType { get; init; } = "Undefined";

    /// <summary>
    /// Gets or sets the correlation identifier that links related events.
    /// </summary>
    public Guid CorrelationId { get; set; }

    /// <summary>
    /// Gets the JSON message with event-specific details.
    /// </summary>
    public virtual string EventMessage { get; } = string.Empty;

    /// <summary>
    /// Gets or sets the timestamp when the event occurred.
    /// </summary>
    public DateTimeOffset EventTimestamp { get; set; } = DateTimeOffset.UtcNow;

    private readonly static JsonSerializerOptions messageJsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        // Allows case-insensitive property matching (for example, "TokenId" and "tokenId").
        PropertyNameCaseInsensitive = true,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull
    };

    /// <summary>
    /// Builds a compact JSON payload string for an event message.
    /// </summary>
    public static string BuildConciseMessage(IPayload payload)
    {
        ArgumentNullException.ThrowIfNull(payload);
        return JsonSerializer.Serialize((object)payload, messageJsonOptions);
    }

    /// <summary>
    /// Deserializes <see cref="EventMessage"/> into the requested payload type.
    /// </summary>
    public TPayload? ReadMessage<TPayload>() where TPayload : IPayload
    {
        if (string.IsNullOrWhiteSpace(EventMessage))
        {
            return default;
        }

        try
        {
            return JsonSerializer.Deserialize<TPayload>(EventMessage, messageJsonOptions);

        }
        catch (Exception ex)
        {
            throw new FieldAccessException("Message has incorrect format", ex);
        }
    }


    public override bool Equals(object? obj)
    {
        if (ReferenceEquals(this, obj))
        {
            return true;
        }

        if (obj is not EventFact other)
        {
            return false;
        }

        return EventStoreSequenceId == other.EventStoreSequenceId
            && EventTimestamp.Equals(other.EventTimestamp);
    }

    public override int GetHashCode() => HashCode.Combine(EventStoreSequenceId, EventTimestamp);

    private sealed class NullDocumentExporterEvent : EventFact
    {
        public NullDocumentExporterEvent()
        {
            EventStoreSequenceId = long.MinValue;
            AggregateSequenceId = int.MinValue;
            EventType = "Undefined";
            EventTimestamp = DateTimeOffset.MinValue;
            CorrelationId = Guid.Empty;
        }
    }
}
