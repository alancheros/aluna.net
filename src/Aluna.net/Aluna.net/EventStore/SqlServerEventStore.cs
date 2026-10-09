using Dapper;
using Aluna.Abstractions;
using Microsoft.Data.SqlClient;
using System.Text.Json;

namespace Aluna.EventStore;

public partial class SqlServerEventStore : IAlunaEventStore
{
    private const string DefaultTableName = "Events";
    private const string SchemaName = "eventstore";
    private const string RawMessagePropertyName = "__rawEventMessage";
    private readonly string _connectionString;
    private readonly string _qualifiedTableName;

    private readonly IEventFactFactory _eventFactory;

    public AppendResult AppendEvents(AggregateStreamKey streamId, IEnumerable<EventFact> events, long expectedAggregateSequence = -1)
    {
        ArgumentNullException.ThrowIfNull(streamId);
        ArgumentNullException.ThrowIfNull(events);

        if (string.IsNullOrWhiteSpace(streamId.StreamName))
        {
            throw new ArgumentException("Stream name is required.", nameof(streamId));
        }

        var batch = events.ToArray();

        using var connection = OpenConnection();
        using var transaction = connection.BeginTransaction();

        var getCurrentSequenceSql = $"""
            SELECT ISNULL(MAX(CAST([AggregateSequence] AS BIGINT)), -1)
            FROM {_qualifiedTableName}
            WHERE [StreamType] = @StreamType
                AND [AggregateId] = @AggregateId;
            """;

        var currentSequence = connection.ExecuteScalar<long>(
            getCurrentSequenceSql,
            new { StreamType = streamId.StreamName, AggregateId = streamId.AggregateId },
            transaction);

        if (expectedAggregateSequence >= 0 && expectedAggregateSequence != currentSequence)
        {
            throw new InvalidOperationException($"Concurrency conflict on stream '{streamId.StreamName}:{streamId.AggregateId}'. Expected last id {expectedAggregateSequence}, actual {currentSequence}.");
        }

        if (batch.Length == 0)
        {
            transaction.Commit();
            return new AppendResult(currentSequence, 0, true, string.Empty);
        }


        var noQueryInsertSql = $"""
            INSERT INTO {_qualifiedTableName}
                ([EventId], [AggregateId], [StreamType], [EventType], [AggregateSequence], [OccurredUtc], [Payload], [MessageVersion], [Metadata], [CorrelationId], [CausationId], [UserId])
            VALUES
                (@EventId, @AggregateId, @StreamType, @EventType, @AggregateSequence, @OccurredUtc, @Payload, @MessageVersion, @Metadata, @CorrelationId, @CausationId, @UserId);
            """;

        var insertSql = $"""
            DECLARE @Inserted TABLE ([EventSequenceId] BIGINT);

            INSERT INTO {_qualifiedTableName}
                ([EventId], [AggregateId], [StreamType], [EventType], [AggregateSequence], [OccurredUtc], [Payload], [MessageVersion], [Metadata], [CorrelationId], [CausationId], [UserId])
            OUTPUT INSERTED.[EventSequenceId] INTO @Inserted([EventSequenceId])
            VALUES
                (@EventId, @AggregateId, @StreamType, @EventType, @AggregateSequence, @OccurredUtc, @Payload, @MessageVersion, @Metadata, @CorrelationId, @CausationId, @UserId);

            SELECT MAX([EventSequenceId]) FROM @Inserted;
            """;

        var rows = new List<object>(batch.Length);
        for (var index = 0; index < batch.Length; index++)
        {
            var source = batch[index];
            var streamVersionLong = currentSequence + index + 1;
            if (streamVersionLong > int.MaxValue)
            {
                throw new InvalidOperationException($"Stream version overflow for stream '{streamId.StreamName}:{streamId.AggregateId}'.");
            }

            rows.Add(new
            {
                EventId = Guid.NewGuid(),
                AggregateId = streamId.AggregateId,
                StreamType = streamId.StreamName,
                EventType = source.EventType,
                AggregateSequence = (int)streamVersionLong,
                OccurredUtc = source.EventTimestamp.UtcDateTime,
                MessageVersion = source.Version,
                Payload = NormalizePayloadForStorage(source.EventMessage),
                Metadata = (string?)null,
                CorrelationId = source.CorrelationId,
                CausationId = (Guid?)null,
                UserId = source.UserId
            });
        }

        if (rows.Count > 1)
        {
            connection.Execute(noQueryInsertSql, rows[..^1], transaction);
        }

        // if you keep multi-row execution, do per-row and keep the latest:
        long lastEventId = -1;

        lastEventId = connection.QuerySingle<long>(insertSql, rows[^1], transaction);


        transaction.Commit();
        return new AppendResult(lastEventId, batch.Length, true, string.Empty);
    }

    public IEnumerable<EventFact> GetEventsForAggregate(AggregateStreamKey streamId)
    {
        ArgumentNullException.ThrowIfNull(streamId);

        if (string.IsNullOrWhiteSpace(streamId.StreamName))
        {
            throw new ArgumentException("Stream name is required.", nameof(streamId));
        }

        var sql = $"""
            SELECT [EventSequenceId], [AggregateId], [StreamType], [AggregateSequence], [EventType], [OccurredUtc], [Payload], [CorrelationId], [UserId]
            FROM {_qualifiedTableName}
            WHERE [StreamType] = @StreamType
                AND [AggregateId] = @AggregateId
            ORDER BY [AggregateSequence] ASC, [EventSequenceId] ASC;
            """;

        using var connection = OpenConnection();
        var records = connection.Query<EventRecord>(sql, new { StreamType = streamId.StreamName, AggregateId = streamId.AggregateId });

        return records
            .Select(ToDomainEventFact)
            .ToArray();
    }

    public IEnumerable<EventFact> ReadEvents(string streamName, long fromEventId = 0, int maxCount = 100)
    {
        if (string.IsNullOrWhiteSpace(streamName))
        {
            throw new ArgumentException("Stream name is required.", nameof(streamName));
        }

        if (maxCount <= 0)
        {
            return Array.Empty<EventFact>();
        }

        var sql = $"""
            SELECT TOP (@MaxCount) [EventSequenceId], [AggregateId], [StreamType], [AggregateSequence], [EventType], [OccurredUtc], [Payload], [MessageVersion], [CorrelationId], [UserId]
            FROM {_qualifiedTableName}
            WHERE [StreamType] = @StreamType
                AND [EventSequenceId] >= @FromEventId
            ORDER BY [AggregateSequence] ASC, [EventSequenceId] ASC;
            """;

        using var connection = OpenConnection();
        var records = connection.Query<EventRecord>(sql, new { StreamType = streamName, FromEventId = fromEventId, MaxCount = maxCount });

        return records
            .Select(ToStoredAggregateEventFact)
            .ToArray();
    }

    private EventFact ToStoredAggregateEventFact(EventRecord row)
    {
        return new StoredAggregateEventFact(
            aggregateId: row.AggregateId, 
            domainEvent: ToDomainEventFact(row), 
            storeSequence: row.EventSequenceId, 
            aggregateSequence: row.AggregateSequence);
    }

    private EventFact ToDomainEventFact(EventRecord row)
    {
        var eventFact = _eventFactory.CreateEventFact(row);

        if (eventFact != null) { return eventFact; }

        return new StoredEventFact(
            storeSequence: row.EventSequenceId,
            aggregateSequence: row.AggregateSequence,
            eventType: row.EventType,
            eventTimestamp: DateTime.SpecifyKind(row.OccurredUtc, DateTimeKind.Utc),
            eventTraceId: row.CorrelationId ?? Guid.Empty,
            eventMessage: DenormalizePayloadFromStorage(row.Payload),
            userId: row.UserId);
    }

    private sealed class StoredAggregateEventFact : EventFact, IStoredAggregateEvent
    {
        public Guid AggregateId { get; }
        public EventFact DomainEvent { get; }

        public override string EventType => DomainEvent.EventType;
        public override string EventMessage => DomainEvent.EventMessage;

        public StoredAggregateEventFact(Guid aggregateId, EventFact domainEvent, long storeSequence, int aggregateSequence)
        {
            AggregateId = aggregateId;
            DomainEvent = domainEvent;
            StoreSequence = storeSequence;
            AggregateSequence = aggregateSequence;
            CorrelationId = domainEvent.CorrelationId;
            EventTimestamp = domainEvent.EventTimestamp;
            UserId = domainEvent.UserId;
        }
    }

    private static string NormalizePayloadForStorage(string payload)
    {
        if (string.IsNullOrWhiteSpace(payload))
        {
            return "{}";
        }

        try
        {
            if (!payload.StartsWith('{'))
            {
                return $"{{\"{RawMessagePropertyName}\":\"{payload}\"}}";
            }

            using var document = JsonDocument.Parse(payload);
            var kind = document.RootElement.ValueKind;

            if (kind == JsonValueKind.Object || kind == JsonValueKind.Array)
            {
                return payload;
            }

            return $"{{\"{RawMessagePropertyName}\":{payload}}}";
        }
        catch (JsonException)
        {
            return JsonSerializer.Serialize(new Dictionary<string, string>
            {
                [RawMessagePropertyName] = payload
            });
        }
    }

    private static string DenormalizePayloadFromStorage(string payload)
    {
        if (string.IsNullOrWhiteSpace(payload))
        {
            return string.Empty;
        }

        try
        {
            using var document = JsonDocument.Parse(payload);
            if (document.RootElement.ValueKind == JsonValueKind.Object
                && document.RootElement.TryGetProperty(RawMessagePropertyName, out var rawMessageProperty))
            {
                if (rawMessageProperty.ValueKind == JsonValueKind.String)
                {
                    return rawMessageProperty.GetString() ?? string.Empty;
                }

                return rawMessageProperty.GetRawText();
            }

            return payload;
        }
        catch (JsonException)
        {
            return payload;
        }
    }

    private SqlConnection OpenConnection()
    {
        var connection = new SqlConnection(_connectionString);
        connection.Open();
        return connection;
    }

    public SqlServerEventStore(string connectionString, IEventFactFactory eventFactory) : this(connectionString, DefaultTableName, eventFactory)
    {
    }

    public SqlServerEventStore(string connectionString, string tableName, IEventFactFactory eventFactory)
    {
        _eventFactory = eventFactory ?? throw new ArgumentNullException(nameof(eventFactory));

        if (string.IsNullOrWhiteSpace(connectionString))
        {
            throw new ArgumentException("A SQL Server connection string is required.", nameof(connectionString));
        }

        if (string.IsNullOrWhiteSpace(tableName))
        {
            throw new ArgumentException("A table name is required.", nameof(tableName));
        }

        _connectionString = connectionString;

        var validatedTableName = ValidateSqlIdentifier(tableName, nameof(tableName), "Table");
        _qualifiedTableName = $"{SchemaName}.{QuoteIdentifier(validatedTableName)}";
    }

    private static string ValidateSqlIdentifier(string value, string argumentName, string entity)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            throw new ArgumentException($"{entity} name is required.", argumentName);
        }

        if (!char.IsLetter(value[0]) && value[0] != '_')
        {
            throw new ArgumentException($"{entity} name must start with a letter or underscore.", argumentName);
        }

        foreach (var character in value)
        {
            if (!char.IsLetterOrDigit(character) && character != '_')
            {
                throw new ArgumentException($"{entity} name can contain only letters, numbers, and underscore.", argumentName);
            }
        }

        return value;
    }

    private static string QuoteIdentifier(string identifier)
    {
        return $"[{identifier.Replace("]", "]]")}]";
    }

    public long GetSequenceByEventId(Guid eventId)
    {
        if (eventId == Guid.Empty)
        {
            throw new ArgumentException("Event ID cannot be empty.", nameof(eventId));
        }

        using var connection = OpenConnection();
        using var command = connection.CreateCommand();
        command.CommandText = $@"
            SELECT [EventSequenceId]
            FROM {_qualifiedTableName}
            WHERE EventId = @EventId
        ";
        command.Parameters.AddWithValue("@EventId", eventId);

        var result = command.ExecuteScalar();
        if (result == null || result == DBNull.Value)
        {
            throw new InvalidOperationException($"Event with ID {eventId} not found.");
        }

        return Convert.ToInt64(result);
    }
}
