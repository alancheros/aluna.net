using Aluna.EventStore;
using Microsoft.Extensions.Logging;
using Aluna.Abstractions;

namespace Aluna.EventStore;

public sealed class LoggingEventStoreDecorator : IAlunaEventStore
{
    private readonly IAlunaEventStore _inner;
    private readonly ILogger<LoggingEventStoreDecorator> _logger;


    public AppendResult AppendEvents(AggregateStreamKey streamId, IEnumerable<EventFact> events, long expectedAggregateSequence = -1)
    {
        ArgumentNullException.ThrowIfNull(streamId);
        ArgumentNullException.ThrowIfNull(events);

        using var scope = AlunaEventStoreLogs.BeginAggregateScope(_logger, streamId.StreamName, streamId.AggregateId, expectedAggregateSequence);
        
        AlunaEventStoreLogs.AppendStarted(_logger);

        try
        {
            var result = _inner.AppendEvents(streamId, events, expectedAggregateSequence);
            AlunaEventStoreLogs.AppendCompleted(_logger, result.Success);
            return result;
        }
        catch (Exception ex)
        {
            AlunaEventStoreLogs.AppendFailed(_logger, ex, streamId.StreamName, streamId.AggregateId, expectedAggregateSequence);
            throw;
        }
    }

    public IEnumerable<EventFact> ReadEvents(string streamName, long fromEventId = 0, int maxCount = 100)
    {
        AlunaEventStoreLogs.ReadStarted(_logger, streamName, fromEventId, maxCount);

        try
        {
            var events = _inner.ReadEvents(streamName, fromEventId, maxCount).ToArray();
            AlunaEventStoreLogs.ReadCompleted(_logger, streamName, fromEventId, maxCount, events.Length);
            return events;
        }
        catch (Exception ex)
        {
            AlunaEventStoreLogs.ReadFailed(_logger, ex, streamName, fromEventId, maxCount);
            throw;
        }
    }

    public IEnumerable<EventFact> GetEventsForAggregate(AggregateStreamKey streamId)
    {
        ArgumentNullException.ThrowIfNull(streamId);

        AlunaEventStoreLogs.GetAggregateStarted(_logger, streamId.StreamName, streamId.AggregateId);

        try
        {
            var events = _inner.GetEventsForAggregate(streamId).ToArray();
            AlunaEventStoreLogs.GetAggregateCompleted(_logger, streamId.StreamName, streamId.AggregateId, events.Length);
            return events;
        }
        catch (Exception ex)
        {
            AlunaEventStoreLogs.GetAggregateFailed(_logger, ex, streamId.StreamName, streamId.AggregateId);
            throw;
        }
    }

    public long GetSequenceByEventId(Guid eventId) => _inner.GetSequenceByEventId(eventId);

    public LoggingEventStoreDecorator(IAlunaEventStore inner, ILogger<LoggingEventStoreDecorator> logger)
    {
        _inner = inner ?? throw new ArgumentNullException(nameof(inner));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }
}