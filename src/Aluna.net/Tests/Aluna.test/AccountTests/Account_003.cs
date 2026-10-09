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
        yield return new TransactionEvent(50m) { StoreSequence = 1L };
        yield return new TransactionEvent(25m) { StoreSequence = 2L };
        yield return new TransactionEvent(-5m) { StoreSequence = 3L };
            
    }
    public override Command? When() => new TransactionCommand(accountId, 100);

    [Fact]
    [Trait("Category", "DocumentExportJob")]
    public void AddDocument_WhenJobIsDraft_ShouldAddFirstDocument()
    {
        HasException.Should().BeFalse(caught?.Message);
        var account = repository.GetById(accountId);

        account.Balance.Should().Be(170);
        account.LastEvent.Should().BeOfType<TransactionEvent>();
        account.LastEvent.As<TransactionEvent>().Amount.Should().Be(100m);
        account.SequenceIndices.AggregateSequence.Should().Be(4);
        account.SequenceIndices.StoreSequence.Should().Be(4);
    }
}
