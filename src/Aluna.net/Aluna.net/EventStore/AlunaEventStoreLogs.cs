using Aluna.Logging;
using Microsoft.Extensions.Logging;

namespace Aluna.EventStore;

internal static partial class AlunaEventStoreLogs
{
    private readonly static string _eventSourcingVersion = "1"; //EventSourcingVersionProvider.Current;
    
    public static IDisposable? BeginAggregateScope(ILogger logger, string streamName, Guid aggregateId, long expectedId) 
    {
        return logger.BeginScope(new Dictionary<string, object>     
        {
            [EventSourcingVersion.Key] = _eventSourcingVersion,
            [StreamName.Key] = streamName,
            [AggregateId.Key] = aggregateId,
            [ExpectedId.Key] = expectedId
        });
    }
    
    [LoggerMessage(EventId = 1000, Level = LogLevel.Debug, Message = "event-append-started")]
    public static partial void AppendStarted(ILogger logger);

    public static void AppendCompleted(ILogger logger, bool success)
    {
        using var _ = logger.BeginScope(new Dictionary<string, object> 
        {
            [OperationSucceeded.Key] = success
        });
        InternalAppendCompleted(logger);
    }

    [LoggerMessage(EventId = 1001, Level = LogLevel.Information, Message = "event-append-completed")]
    private static partial void InternalAppendCompleted(ILogger logger);
    
    

    [LoggerMessage(EventId = 1002, Level = LogLevel.Error, Message = "Append failed. Stream={StreamName}, AggregateId={AggregateId}, ExpectedId={ExpectedId}")]
    public static partial void AppendFailed(ILogger logger, Exception exception, string streamName, Guid aggregateId, long expectedId);

    [LoggerMessage(EventId = 1010, Level = LogLevel.Debug, Message = "Reading events. Stream={StreamName}, FromEventId={FromEventId}, MaxCount={MaxCount}")]
    public static partial void ReadStarted(ILogger logger, string streamName, long fromEventId, int maxCount);

    [LoggerMessage(EventId = 1011, Level = LogLevel.Information, Message = "Read completed. Stream={StreamName}, FromEventId={FromEventId}, MaxCount={MaxCount}, Returned={ReturnedCount}")]
    public static partial void ReadCompleted(ILogger logger, string streamName, long fromEventId, int maxCount, int returnedCount);

    [LoggerMessage(EventId = 1012, Level = LogLevel.Error, Message = "Read failed. Stream={StreamName}, FromEventId={FromEventId}, MaxCount={MaxCount}")]
    public static partial void ReadFailed(ILogger logger, Exception exception, string streamName, long fromEventId, int maxCount);

    [LoggerMessage(EventId = 1020, Level = LogLevel.Debug, Message = "Loading aggregate events. Stream={StreamName}, AggregateId={AggregateId}")]
    public static partial void GetAggregateStarted(ILogger logger, string streamName, Guid aggregateId);

    [LoggerMessage(EventId = 1021, Level = LogLevel.Information, Message = "Aggregate events loaded. Stream={StreamName}, AggregateId={AggregateId}, Returned={ReturnedCount}")]
    public static partial void GetAggregateCompleted(ILogger logger, string streamName, Guid aggregateId, int returnedCount);

    [LoggerMessage(EventId = 1022, Level = LogLevel.Error, Message = "Loading aggregate events failed. Stream={StreamName}, AggregateId={AggregateId}")]
    public static partial void GetAggregateFailed(ILogger logger, Exception exception, string streamName, Guid aggregateId);
    
    public static LoggingAttribute<string> EventSourcingVersion => new LoggingAttribute<string>("aluna.event_sourcing_version");
    
    public static LoggingAttribute<string> StreamName => new LoggingAttribute<string>("aluna.stream_name");
    public static LoggingAttribute<Guid> AggregateId => new LoggingAttribute<Guid>("aluna.aggregate_id");
    public static LoggingAttribute<long> ExpectedId => new LoggingAttribute<long>("aluna.expected_id");
    
    public static LoggingAttribute<bool> OperationSucceeded => new LoggingAttribute<bool>("aluna.operation_succeeded");
    
}