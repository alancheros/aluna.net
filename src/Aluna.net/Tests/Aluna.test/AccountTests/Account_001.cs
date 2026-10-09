
using Aluna.Abstractions;
using Aluna.DomainDemo;
using Aluna.DomainDemo.Commands;
using Aluna.test.Abstractions;
using FluentAssertions;
using System.Runtime.InteropServices.JavaScript;

namespace Aluna.test.AccountTests
{
    public class Account_001 : AccountTestBase
    {
        private readonly Guid jobId = Guid.NewGuid();

        [Fact]
        [Trait("Category", "DocumentExportJob")]
        [Trait("Path", "Happy Path")]
        public void CreateJob_WhenCommandIsHandled_ShouldCreateExportJob()
        {
            HasException.Should().BeFalse();
            var account = repository.GetById(jobId);
            account.Should().NotBeNull();
            account.StreamKey.AggregateId.Should().Be(jobId);
            account.StreamKey.StreamName.Should().Be(Account.STREAM_NAME);
            account.CreatedAtUtc.Should().BeCloseTo(DateTimeOffset.UtcNow, TimeSpan.FromSeconds(1));
            account.SequenceIndices.AggregateSequence.Should().Be(0);
            account.SequenceIndices.StoreSequence.Should().Be(1L);
        }

        public override IEnumerable<EventFact> Given() => [];

        public override Command When() => new CreateAccountCommand(jobId, "LuisLancheros" );
    }
}
