using System.Data.SqlClient;
using System.Data.SQLite;
using Datalite.Destination;
using Datalite.Exceptions;
using Datalite.Testing;
using FluentAssertions;
using Microsoft.Data.Sqlite;
using Xunit;

namespace Datalite.Sources.Databases.Sqlite.Tests.Unit
{
    public class SqliteExtensionsTests : TestBaseClass
    {
        [Fact]
        public async void ExtensionWithMicrosoftConnectionAccepted()
        {
            await WithSqliteInMemoryConnection(conn =>
            {
                var builder = conn.Add().FromSqlite(conn);
                builder.Should().NotBeNull();
                return Task.CompletedTask;
            });
        }

        [Fact]
        public async void ExtensionWithSystemDataConnectionAccepted()
        {
            await using var connection = new SQLiteConnection("Data Source=:memory:");
            await connection.OpenAsync();

            await WithSqliteInMemoryConnection(conn =>
            {
                var builder = conn.Add().FromSqlite(connection);
                builder.Should().NotBeNull();
                return Task.CompletedTask;
            });
        }

        [Fact]
        public async void ExtensionWithNullConnectionStringRejected()
        {
            await WithSqliteInMemoryConnection(conn =>
            {
                conn
                    .Add()
                    .Invoking(x => x.FromSqlite(string.Empty))
                    .Should()
                    .Throw<DataliteException>()
                    .WithMessage("A valid Sqlite connection string, Microsoft.Data.Sqlite.SqliteConnection object or System.Data.SQLite.SQLiteConnection object must be provided.");
                return Task.CompletedTask;
            });
        }

        [Fact]
        public async void ExtensionWithNullMicrosoftConnectionRejected()
        {
            SqliteConnection? connection = null;

            await WithSqliteInMemoryConnection(conn =>
            {
                conn
                    .Add()
                    .Invoking(x => x.FromSqlite(connection!))
                    .Should()
                    .Throw<DataliteException>()
                    .WithMessage("A valid Sqlite connection string, Microsoft.Data.Sqlite.SqliteConnection object or System.Data.SQLite.SQLiteConnection object must be provided.");
                return Task.CompletedTask;
            });
        }

        [Fact]
        public async void ExtensionWithNullSystemConnectionRejected()
        {
            SQLiteConnection? connection = null;

            await WithSqliteInMemoryConnection(conn =>
            {
                conn
                    .Add()
                    .Invoking(x => x.FromSqlite(connection!))
                    .Should()
                    .Throw<DataliteException>()
                    .WithMessage("A valid Sqlite connection string, Microsoft.Data.Sqlite.SqliteConnection object or System.Data.SQLite.SQLiteConnection object must be provided.");
                return Task.CompletedTask;
            });
        }

        [Fact]
        public async void ExtensionWithSqlServerConnectionRejected()
        {
            // This is a SQL Server connection.
            var connection = new SqlConnection();

            await WithSqliteInMemoryConnection(conn =>
            {
                conn
                    .Add()
                    .Invoking(x => x.FromSqlite(connection))
                    .Should()
                    .Throw<ArgumentOutOfRangeException>()
                    .WithMessage("The connection must either be a Microsoft.Data.Sqlite.SqliteConnection or a System.Data.SQLite.SQLiteConnection (Parameter 'sqlConnection')");
                return Task.CompletedTask;
            });
        }
    }
}
