using System.Threading.Tasks;
using Datalite.Destination;
using Datalite.Exceptions;
using Datalite.Testing;
using FluentAssertions;
using Xunit;

namespace Datalite.Sources.Databases.AzureTables.Tests.Unit
{
    public class AzureTableExtensionsTests : TestBaseClass
    {
        [Fact]
        public async void NullConnectionStringRejected()
        {
            await WithSqliteInMemoryConnection(conn =>
            {
                conn
                    .Add()
                    .Invoking(x => x.FromAzureTables(string.Empty))
                    .Should()
                    .Throw<DataliteException>()
                    .WithMessage("An Azure Table Storage connection string must be provided.");

                return Task.CompletedTask;
            });
        }
    }
}
