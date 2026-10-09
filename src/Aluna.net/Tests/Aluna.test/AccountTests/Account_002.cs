using Aluna.Abstractions;
using Aluna.DomainDemo.Commands;
using Aluna.test.Abstractions;
using Aluna.DomainDemo.Events;
using FluentAssertions;

namespace Aluna.test.AccountTests;

/// <summary>
/// Caso de prueba: Cuando se crea una cuenta y se realiza una transacción, se debe obtener el nuevo saldo de la cuenta correctamente.
/// </summary>
public class Account_002 : AccountTestBase
{
    private readonly Guid accountId = Guid.NewGuid();

    public override IEnumerable<EventFact> Given()
    {
        yield return new AccountCreatedEvent(accountId);
    }

    public override Command? When() => new TransactionCommand(accountId, 100) { ExpectedAggregateSequence = 1 };

    [Fact]
    [Trait("Category", "DocumentExportJob")]
    public void AddDocument_WhenJobIsDraft_ShouldAddFirstDocument()
    {
        HasException.Should().BeFalse(caught?.Message);
        repository.GetById(accountId).Balance.Should().Be(100);
    }
}