using Microsoft.Extensions.Logging;

namespace Aluna.EventStore;

internal static partial class ParadocEventStoreLogs
{
    [LoggerMessage(EventId = 1000, Level = LogLevel.Debug, Message = "Appending events. Stream={StreamName}, AggregateId={AggregateId}, ExpectedId={ExpectedId}")]
    public static partial void AppendStarted(ILogger logger, string streamName, Guid aggregateId, long expectedId);

    [LoggerMessage(EventId = 1001, Level = LogLevel.Information, Message = "Append completed. Stream={StreamName}, AggregateId={AggregateId}, Appended={AppendedCount}, LastEventId={LastEventId}, Success={Success}")]
    public static partial void AppendCompleted(ILogger logger, string streamName, Guid aggregateId, int appendedCount, long lastEventId, bool success);

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
}