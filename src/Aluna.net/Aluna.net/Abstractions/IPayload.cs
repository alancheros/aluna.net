namespace Aluna.Abstractions;

/// <summary>
/// Defines the minimal contract for event payload objects.
/// </summary>
public interface IPayload
{
    /// <summary>
    /// Gets the payload contract version.
    /// </summary>
    public int ContractVersion { get; init; }
}

/// <summary>
/// Represents an empty payload.
/// </summary>
public record NullPayload : IPayload
{
    public int ContractVersion { get; init; } = 1;
}