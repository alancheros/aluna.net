namespace Aluna.EventStore;

public record AppendResult
{
    public long LastEventSequence { get; init; }
    public bool Success { get; init; }
    public string? ErrorMessage { get; init; }
    public int AppendedCount { get; init; }
    public AppendResult(long lastEventSequence, int appendedCount, bool success, string errorMessage)
    {
        LastEventSequence = lastEventSequence;
        Success = success;
        ErrorMessage = errorMessage;
        AppendedCount = appendedCount;
    }
}