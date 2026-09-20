namespace Aluna.Abstractions;

/// <summary>
/// Represents the result of a document generation operation.
/// </summary>
public class DocumentResult
{
    /// <summary>
    /// Gets or sets the current document processing status.
    /// </summary>
    public DocumentStatus Status { get; set; }

    /// <summary>
    /// Gets or sets the error message when generation fails.
    /// </summary>
    public string ErrorMessage { get; set; } = string.Empty;

    /// <summary>
    /// Gets or sets the generated document path.
    /// </summary>
    public string DocumentPath { get; set; } = string.Empty;
}
