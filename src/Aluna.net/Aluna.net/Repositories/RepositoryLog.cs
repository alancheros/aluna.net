using Microsoft.Extensions.Logging;

namespace Aluna.Repositories;

internal static partial class RepositoryLog
{
    [LoggerMessage(
        EventId = 2000,
        Level = LogLevel.Information,
        Message = "Save started for aggregate {AggregateType} with id {AggregateId} in stream {StreamName}. expectedId={ExpectedId}")]
    internal static partial void SaveStarted(
        ILogger logger,
        string aggregateType,
        Guid aggregateId,
        string streamName,
        long expectedId);

    [LoggerMessage(
        EventId = 2001,
        Level = LogLevel.Information,
        Message = "Save completed for aggregate {AggregateType} with id {AggregateId} in stream {StreamName}. elapsedMs={ElapsedMs}")]
    internal static partial void SaveCompleted(
        ILogger logger,
        string aggregateType,
        Guid aggregateId,
        string streamName,
        double elapsedMs);

    [LoggerMessage(
        EventId = 2002,
        Level = LogLevel.Error,
        Message = "Save failed for aggregate {AggregateType} with id {AggregateId} in stream {StreamName}. expectedId={ExpectedId}. elapsedMs={ElapsedMs}")]
    internal static partial void SaveFailed(
        ILogger logger,
        string aggregateType,
        Guid aggregateId,
        string streamName,
        long expectedId,
        double elapsedMs,
        Exception exception);

    [LoggerMessage(
        EventId = 2010,
        Level = LogLevel.Information,
        Message = "GetById started for aggregate {AggregateType} with id {AggregateId} in stream {StreamName}")]
    internal static partial void GetByIdStarted(
        ILogger logger,
        string aggregateType,
        Guid aggregateId,
        string streamName);

    [LoggerMessage(
        EventId = 2011,
        Level = LogLevel.Information,
        Message = "GetById completed for aggregate {AggregateType} with id {AggregateId} in stream {StreamName}. elapsedMs={ElapsedMs}")]
    internal static partial void GetByIdCompleted(
        ILogger logger,
        string aggregateType,
        Guid aggregateId,
        string streamName,
        double elapsedMs);

    [LoggerMessage(
        EventId = 2012,
        Level = LogLevel.Warning,
        Message = "GetById not found for aggregate {AggregateType} with id {AggregateId} in stream {StreamName}. elapsedMs={ElapsedMs}")]
    internal static partial void GetByIdNotFound(
        ILogger logger,
        string aggregateType,
        Guid aggregateId,
        string streamName,
        double elapsedMs);

    [LoggerMessage(
        EventId = 2020,
        Level = LogLevel.Information,
        Message = "MakeNew started for aggregate {AggregateType} in stream {StreamName}")]
    internal static partial void MakeNewStarted(
        ILogger logger,
        string aggregateType,
        string streamName);

    [LoggerMessage(
        EventId = 2021,
        Level = LogLevel.Information,
        Message = "MakeNew completed for aggregate {AggregateType} with id {AggregateId} in stream {StreamName}")]
    internal static partial void MakeNewCompleted(
        ILogger logger,
        string aggregateType,
        Guid aggregateId,
        string streamName);

    [LoggerMessage(
        EventId = 2030,
        Level = LogLevel.Information,
        Message = "RefreshMemoryCache started for aggregate {AggregateType} in stream {StreamName}")]
    internal static partial void RefreshMemoryCacheStarted(
        ILogger logger,
        string aggregateType,
        string streamName);

    [LoggerMessage(
        EventId = 2031,
        Level = LogLevel.Information,
        Message = "RefreshMemoryCache completed for aggregate {AggregateType} in stream {StreamName}. elapsedMs={ElapsedMs}")]
    internal static partial void RefreshMemoryCacheCompleted(
        ILogger logger,
        string aggregateType,
        string streamName,
        double elapsedMs);

    [LoggerMessage(
        EventId = 2040,
        Level = LogLevel.Information,
        Message = "Attach started for aggregate {AggregateType} with id {AggregateId} in stream {StreamName}")]
    internal static partial void AttachStarted(
        ILogger logger,
        string aggregateType,
        Guid aggregateId,
        string streamName);

    [LoggerMessage(
        EventId = 2041,
        Level = LogLevel.Information,
        Message = "Attach completed for aggregate {AggregateType} with id {AggregateId} in stream {StreamName}")]
    internal static partial void AttachCompleted(
        ILogger logger,
        string aggregateType,
        Guid aggregateId,
        string streamName);
}
