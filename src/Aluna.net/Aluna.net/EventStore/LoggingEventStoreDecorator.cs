using Aluna.EventStore;
using Microsoft.Extensions.Logging;
using Aluna.Abstractions;

namespace Aluna.EventStore;

public sealed class LoggingEventStoreDecorator : IAlunaEventStore
{
    private readonly IAlunaEventStore _inner;
    private readonly ILogger<LoggingEventStoreDecorator> _logger;
    private readonly string _eventSourcingVersion = "1"; //EventSourcingVersionProvider.Current;


    public AppendResult AppendEvents(AggregateStreamAndId streamId, IEnumerable<EventFact> events, long expectedId = -1)
    {
        ArgumentNullException.ThrowIfNull(streamId);
        ArgumentNullException.ThrowIfNull(events);
        
        using var _ = BeginEventSourcingVersionScope();

        AlunaEventStoreLogs.AppendStarted(_logger, streamId.Name, streamId.AggregateId, expectedId);

        try
        {
            var result = _inner.AppendEvents(streamId, events, expectedId);
            AlunaEventStoreLogs.AppendCompleted(_logger, streamId.Name, streamId.AggregateId, result.AppendedCount, result.LastEventId, result.Success);
            return result;
        }
        catch (Exception ex)
        {
            AlunaEventStoreLogs.AppendFailed(_logger, ex, streamId.Name, streamId.AggregateId, expectedId);
            throw;
        }
    }

    public IEnumerable<EventFact> ReadEvents(string streamName, long fromEventId = 0, int maxCount = 100)
    {
        using var _ = BeginEventSourcingVersionScope();

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

    public IEnumerable<EventFact> GetEventsForAggregate(AggregateStreamAndId streamId)
    {
        using var _ = BeginEventSourcingVersionScope();

        ArgumentNullException.ThrowIfNull(streamId);

        AlunaEventStoreLogs.GetAggregateStarted(_logger, streamId.Name, streamId.AggregateId);

        try
        {
            var events = _inner.GetEventsForAggregate(streamId).ToArray();
            AlunaEventStoreLogs.GetAggregateCompleted(_logger, streamId.Name, streamId.AggregateId, events.Length);
            return events;
        }
        catch (Exception ex)
        {
            AlunaEventStoreLogs.GetAggregateFailed(_logger, ex, streamId.Name, streamId.AggregateId);
            throw;
        }
    }

#pragma warning disable CS8603 // Possible null reference return.
    private IDisposable BeginEventSourcingVersionScope() =>
        _logger.BeginScope(new Dictionary<string, object?>
        {
            ["EventSourcingVersion"] = _eventSourcingVersion
        });
#pragma warning restore CS8603 // Possible null reference return.

    public LoggingEventStoreDecorator(IAlunaEventStore inner, ILogger<LoggingEventStoreDecorator> logger)
    {
        _inner = inner ?? throw new ArgumentNullException(nameof(inner));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }
}
