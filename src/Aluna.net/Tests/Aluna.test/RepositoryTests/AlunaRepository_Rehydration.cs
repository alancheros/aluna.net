using Aluna.DomainDemo;
using Aluna.DomainDemo.Events;
using Aluna.EventStore;
using Aluna.Exceptions;
using Aluna.Abstractions;
using FluentAssertions;

namespace Aluna.test.RepositoryTests;

public class AlunaRepository_Rehydration
{
    [Fact]
    public void RehydrateFromStoreIndex_WhenValidEventsExist_ShouldHydrateAggregatesAndReturnSummary()
    {
        var eventStore = new InMemoryEventStore();
        var repository = new AccountRepository(eventStore);

        var accountId1 = Guid.NewGuid();
        var accountId2 = Guid.NewGuid();

        eventStore.AppendEvents(new AggregateStreamAndId(Account.STREAM_NAME, accountId1),
        [
            new AccountCreatedEvent(accountId1),
            new TransactionEvent(10m)
        ]);

        eventStore.AppendEvents(new AggregateStreamAndId(Account.STREAM_NAME, accountId2),
        [
            new AccountCreatedEvent(accountId2),
            new TransactionEvent(5m),
            new TransactionEvent(-2m)
        ]);

        var summary = repository.RehydrateFromStoreIndex(-1);

        summary.ProcessedEvents.Should().Be(5);
        summary.HydratedAggregates.Should().Be(2);
        summary.SkippedEvents.Should().Be(0);
        summary.LastScannedStoreIndex.Should().Be(4);

        repository.GetById(accountId1).Balance.Should().Be(10m);
        repository.GetById(accountId2).Balance.Should().Be(3m);
    }

    [Fact]
    public void RehydrateFromStoreIndex_WhenAggregateWasNotCreated_ShouldSkipEvents()
    {
        var eventStore = new InMemoryEventStore();
        var repository = new AccountRepository(eventStore);

        var accountId = Guid.NewGuid();
        eventStore.AppendEvents(new AggregateStreamAndId(Account.STREAM_NAME, accountId),
        [
            new TransactionEvent(25m)
        ]);

        var summary = repository.RehydrateFromStoreIndex(-1);

        summary.ProcessedEvents.Should().Be(1);
        summary.HydratedAggregates.Should().Be(0);
        summary.SkippedEvents.Should().Be(1);
        summary.LastScannedStoreIndex.Should().Be(0);
    }

    [Fact]
    public void RehydrateFromStoreIndex_WhenStoreIndexIsProvided_ShouldReadExclusivelyAfterThatIndex()
    {
        var eventStore = new InMemoryEventStore();
        var repository = new AccountRepository(eventStore);

        var accountId = Guid.NewGuid();
        eventStore.AppendEvents(new AggregateStreamAndId(Account.STREAM_NAME, accountId),
        [
            new AccountCreatedEvent(accountId),
            new TransactionEvent(100m)
        ]);

        var summary = repository.RehydrateFromStoreIndex(0);

        summary.ProcessedEvents.Should().Be(1);
        summary.HydratedAggregates.Should().Be(0);
        summary.SkippedEvents.Should().Be(1);
        summary.LastScannedStoreIndex.Should().Be(1);
    }

    [Fact]
    public void RehydrateFromStoreIndex_ShouldReadOnlyRepositoryStream()
    {
        var eventStore = new InMemoryEventStore();
        var repository = new AccountRepository(eventStore);

        var accountId = Guid.NewGuid();
        var otherStreamAggregateId = Guid.NewGuid();

        eventStore.AppendEvents(new AggregateStreamAndId(Account.STREAM_NAME, accountId),
        [
            new AccountCreatedEvent(accountId),
            new TransactionEvent(20m)
        ]);

        eventStore.AppendEvents(new AggregateStreamAndId("OTHER_STREAM", otherStreamAggregateId),
        [
            new AccountCreatedEvent(otherStreamAggregateId)
        ]);

        var summary = repository.RehydrateFromStoreIndex(-1);

        summary.ProcessedEvents.Should().Be(2);
        summary.HydratedAggregates.Should().Be(1);
        summary.SkippedEvents.Should().Be(0);
        summary.LastScannedStoreIndex.Should().Be(1);

        Action readOther = () => repository.GetById(otherStreamAggregateId);
        readOther.Should().Throw<AggregateNotFoundException>();
    }
}
