namespace Aluna.Logging;

public abstract class LoggingAttributeBase
{
    public string Key { get; init; }

    public LoggingAttributeBase(string key)
    {
        Key = key;
    }
}

public class LoggingAttribute<T> : LoggingAttributeBase
{
    public T? Value { get; set; }

    public LoggingAttribute(string key) : base(key)
    {
    }
}