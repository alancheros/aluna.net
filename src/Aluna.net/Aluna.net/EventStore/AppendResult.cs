namespace Aluna.EventStore;

public record AppendResult
{
    public long LastInsertedStoreSequence { get; init; }
    public bool Success { get; init; }
    public string? ErrorMessage { get; init; }
    public int AppendedCount { get; init; }
    public AppendResult(long lastInsertedStoreSequence, int appendedCount, bool success, string errorMessage)
    {
        LastInsertedStoreSequence = lastInsertedStoreSequence;
        Success = success;
        ErrorMessage = errorMessage;
        AppendedCount = appendedCount;
    }
}