namespace Aluna.Repositories;

public sealed record HydrationSummary
{
    public long ProcessedEvents { get; init; }
    public int HydratedAggregates { get; init; }
    public long SkippedEvents { get; init; }
    public long LastScannedStoreIndex { get; init; }

    public HydrationSummary(long processedEvents, int hydratedAggregates, long skippedEvents, long lastScannedStoreIndex)
    {
        ProcessedEvents = processedEvents;
        HydratedAggregates = hydratedAggregates;
        SkippedEvents = skippedEvents;
        LastScannedStoreIndex = lastScannedStoreIndex;
    }
}
