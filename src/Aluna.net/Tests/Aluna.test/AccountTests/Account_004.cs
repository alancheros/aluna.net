using Aluna.Abstractions;
using Aluna.DomainDemo.Events;
using Aluna.test.Abstractions;
using FluentAssertions;

namespace Aluna.test.AccountTests;

public class Account_004 : AccountTestBase
{
    private readonly Guid accountId = Guid.NewGuid();

    public override IEnumerable<EventFact> Given()
    {
        yield return new AccountCreatedEvent(accountId);
        yield return new TransactionEvent(10m) { EventSequenceId = 1L };
        yield return new TransactionEvent(-3m) { EventSequenceId = 2L };
    }

    public override Command? When() => null;

    [Fact]
    [Trait("Category", "DocumentExportJob")]
    public void LoadFromHistory_WhenEventsAreReplayed_ShouldSetLastEventAndAggregateSequence()
    {
        HasException.Should().BeFalse(caught?.Message);

        var account = repository.GetById(accountId);

        account.LastEvent.Should().BeOfType<TransactionEvent>();
        account.LastEvent.As<TransactionEvent>().Amount.Should().Be(-3m);
        account.SequenceIndices.AggregateIndex.Should().Be(2);
    }
}
