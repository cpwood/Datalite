using System.Threading.Tasks;
using Datalite.Destination;
using Datalite.Exceptions;
using Datalite.Testing;
using FluentAssertions;
using Npgsql;
using Xunit;

namespace Datalite.Sources.Databases.Postgres.Tests.Unit
{
    public class PostgresExtensionsTests : TestBaseClass
    {
        [Fact]
        public async void SqlServerConnectionAccepted()
        {
            var connection = new NpgsqlConnection();

            await WithSqliteInMemoryConnection(conn =>
            {
                var builder = conn.Add().FromPostgres(connection);
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
                    .Invoking(x => x.FromPostgres(string.Empty))
                    .Should()
                    .Throw<DataliteException>()
                    .WithMessage("A valid Postgres connection string or NpgsqlConnection object must be provided.");

                return Task.CompletedTask;
            });
        }

        [Fact]
        public async void NullConnectionRejected()
        {
            NpgsqlConnection? connection = null;

            await WithSqliteInMemoryConnection(conn =>
            {
                conn
                    .Add()
                    .Invoking(x => x.FromPostgres(connection!))
                    .Should()
                    .Throw<DataliteException>()
                    .WithMessage("A valid Postgres connection string or NpgsqlConnection object must be provided.");

                return Task.CompletedTask;
            });
        }
    }
}
