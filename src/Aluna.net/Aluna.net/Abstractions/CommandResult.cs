namespace Aluna.Abstractions;

/// <summary>
/// Represents the result of command execution.
/// </summary>
public class CommandResult
{
    /// <summary>
    /// Gets a value indicating whether command execution succeeded.
    /// </summary>
    public bool IsSuccess { get; init; } = true;

    /// <summary>
    /// Gets or sets a value indicating whether an asynchronous job has completed.
    /// </summary>
    public bool IsJobCompleted { get; set; } = false;

    /// <summary>
    /// Gets or sets a value indicating whether command execution was intentionally omitted.
    /// </summary>
    public bool WasOmitted { get; set; } = false;

    /// <summary>
    /// Gets a predefined omitted result instance.
    /// </summary>
    public static CommandResult Omitted { get; set; } = new() { WasOmitted = true, IsSuccess = true };

    /// <summary>
    /// Gets a predefined successful result instance.
    /// </summary>
    public static readonly CommandResult Success = new() { IsSuccess = true };
}
