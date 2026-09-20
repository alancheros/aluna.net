namespace Paradigma.EventSourcing.Abstractions;

/// <summary>
/// Excepción base para errores relacionados con el manejo de eventos en el contexto de Event Sourcing.
/// </summary>
public class EventSourcingException : Exception
{
    public EventSourcingException()
    {
    }

    public EventSourcingException(string? message) : base(message)
    {
    }

    public EventSourcingException(string? message, Exception? innerException) : base(message, innerException)
    {
    }
}
