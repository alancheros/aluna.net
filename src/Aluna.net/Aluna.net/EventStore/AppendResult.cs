namespace Aluna.EventStore;

public record AppendResult
{
    public long LastEventId { get; init; }
    public bool Success { get; init; }
    public string? ErrorMessage { get; init; }
    public int AppendedCount { get; init; }
    public AppendResult(long lastEventId, int appendedCount, bool success, string errorMessage)
    {
        LastEventId = lastEventId;
        Success = success;
        ErrorMessage = errorMessage;
        AppendedCount = appendedCount;
    }
}