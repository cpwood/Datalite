using System.Threading.Tasks;
using Datalite.Destination;
using Datalite.Exceptions;
using Datalite.Testing;
using FluentAssertions;
using Xunit;

namespace Datalite.Sources.Databases.H2.Tests.Unit
{
    public class H2ExtensionsTests : TestBaseClass
    {
        [Fact]
        public async void ExtensionWithNullMicrosoftConnectionRejected()
        {
            H2Connection? connection = null;

            await WithSqliteInMemoryConnection(conn =>
            {
                conn
                    .Add()
                    .Invoking(x => x.FromH2(connection!))
                    .Should()
                    .Throw<DataliteException>()
                    .WithMessage("A valid H2Connection object must be provided.");
                return Task.CompletedTask;
            });
        }
    }
}
