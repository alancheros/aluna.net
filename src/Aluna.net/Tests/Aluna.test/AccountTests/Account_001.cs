
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
            account.Id.AggregateId.Should().Be(jobId);
            account.Id.Name.Should().Be(Account.STREAM_NAME);
            account.CreatedAtUtc.Should().BeCloseTo(DateTimeOffset.UtcNow, TimeSpan.FromSeconds(1));
            account.AggregateSequence.Should().Be(0);
        }

        public override IEnumerable<EventFact> Given() => [];

        public override Command When() => new CreateAccountCommand(jobId, "LuisLancheros" );
    }
}
