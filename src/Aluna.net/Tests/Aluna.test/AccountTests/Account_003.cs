using Aluna.Abstractions;
using Aluna.DomainDemo.Commands;
using Aluna.test.Abstractions;
using Aluna.DomainDemo.Events;
using FluentAssertions;

namespace Aluna.test.AccountTests;

/// <summary>
/// Caso de prueba: Cuando se crea una cuenta y se realizan múltiples transacciones, se debe obtener el saldo final de la cuenta correctamente.
/// </summary>
public class Account_003 : AccountTestBase
{
    private readonly Guid accountId = Guid.NewGuid();
    public override IEnumerable<EventFact> Given()
    {
        yield return new AccountCreatedEvent(accountId);
        yield return new TransactionEvent(50m);
        yield return new TransactionEvent(25m);
        yield return new TransactionEvent(-5m);
            
    }
    public override Command? When() => new TransactionCommand(accountId, 100);

    [Fact]
    [Trait("Category", "DocumentExportJob")]
    public void AddDocument_WhenJobIsDraft_ShouldAddFirstDocument()
    {
        HasException.Should().BeFalse(caught?.Message);
        repository.GetById(accountId).Balance.Should().Be(170);
    }
}
