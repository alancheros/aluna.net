namespace Aluna.Abstractions;

/// <summary>
/// Defines lifecycle states for a document export job.
/// </summary>
public enum DocumentExportJobStatus
{
    Undefined = 0,
    Draft = 1,
    Queued = 2,
    Running = 3,
    Completed = 4,
    Failed = 5,
    Canceled = 6,
    Deleted = 7,
    Prepared = 8,
    Packaged = 9,
    Expired = 10
}

/// <summary>
/// Defines processing states for a generated document.
/// </summary>
public enum DocumentStatus
{
    Undefined = 0,
    Pending = 1,
    Queued = 2,
    Running = 3,
    Failed = 4,
    Created = 5
}
