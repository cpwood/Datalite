using System.Data.SqlClient;
using System.Threading.Tasks;
using Datalite.Destination;
using Datalite.Exceptions;
using Datalite.Testing;
using FluentAssertions;
using Xunit;

namespace Datalite.Sources.Databases.SqlServer.Tests.Unit
{
    public class SqlServerExtensionsTests : TestBaseClass
    {
        [Fact]
        public async void SqlServerConnectionAccepted()
        {
            var connection = new SqlConnection();

            await WithSqliteInMemoryConnection(conn =>
            {
                var builder = conn.Add().FromSqlServer(connection);
                builder.Should().NotBeNull();
                return Task.CompletedTask;
            });
        }

        [Fact]
        public async void NullConnectionStringRejected()
        {
            await WithSqliteInMemoryConnection(conn =>
            {
                conn
                    .Add()
                    .Invoking(x => x.FromSqlServer(string.Empty))
                    .Should()
                    .Throw<DataliteException>()
                    .WithMessage("A valid SQL Server connection string or SqlConnection object must be provided.");

                return Task.CompletedTask;
            });
        }

        [Fact]
        public async void NullConnectionRejected()
        {
            SqlConnection? connection = null;

            await WithSqliteInMemoryConnection(conn =>
            {
                conn
                    .Add()
                    .Invoking(x => x.FromSqlServer(connection!))
                    .Should()
                    .Throw<DataliteException>()
                    .WithMessage("A valid SQL Server connection string or SqlConnection object must be provided.");

                return Task.CompletedTask;
            });
        }
    }
}
