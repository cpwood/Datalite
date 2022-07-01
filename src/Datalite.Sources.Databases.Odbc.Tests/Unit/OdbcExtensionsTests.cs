using System.Data.Odbc;
using System.Threading.Tasks;
using Datalite.Destination;
using Datalite.Exceptions;
using Datalite.Testing;
using FluentAssertions;
using Xunit;

namespace Datalite.Sources.Databases.Odbc.Tests.Unit
{
    public class OdbcExtensionsTests : TestBaseClass
    {
        [Fact]
        public async void NullConnectionStringRejected()
        {
            await WithSqliteInMemoryConnection(conn =>
            {
                conn
                    .Add()
                    .Invoking(x => x.FromOdbc(string.Empty))
                    .Should()
                    .Throw<DataliteException>()
                    .WithMessage("A valid ODBC connection string or OdbcConnection object must be provided.");

                return Task.CompletedTask;
            });
        }

        [Fact]
        public async void NullConnectionRejected()
        {
            OdbcConnection? connection = null;

            await WithSqliteInMemoryConnection(conn =>
            {
                conn
                    .Add()
                    .Invoking(x => x.FromOdbc(connection!))
                    .Should()
                    .Throw<DataliteException>()
                    .WithMessage("A valid ODBC connection string or OdbcConnection object must be provided.");

                return Task.CompletedTask;
            });
        }
    }
}
