using System;
using Datalite.Destination;
using Datalite.Testing;
using Microsoft.Data.Sqlite;
using System.Data.SqlClient;
using FluentAssertions;
using Xunit;

namespace Datalite.Tests.Destination
{
    public class SqliteConnectionExtensionsTests : TestBaseClass
    {
        [Fact]
        public async void VacuumSuccessful()
        {
            await WithSqliteInMemoryConnection(async connection =>
            {
                await connection.VacuumAsync();
            });
        }

        [Fact]
        public async void TableDroppedSuccessfully()
        {
            await WithSqliteInMemoryConnection(async connection =>
            {
                var definition = new TableDefinition("MyTable");
                definition.Columns.Add("id", new Column("id", typeof(int), true));

                var cmd = new SqliteCommand(definition.ToString(), connection);
                await cmd.ExecuteNonQueryAsync();

                var table = await connection.LoadTableAsync("MyTable");
                table.Should().Exist();

                await connection.DropTableAsync("MyTable");

                table = await connection.LoadTableAsync("MyTable");
                table.Should().NotExist();
            });
        }

        [Fact]
        public void AddOnNonSqliteDatabaseRejected()
        {
            // This is a SQL Server connection.
            var connection = new SqlConnection();
            connection.Invoking(x => x.Add())
                .Should()
                .Throw<ArgumentOutOfRangeException>()
                .WithMessage(
                    "The connection must either be a Microsoft.Data.Sqlite.SqliteConnection or a System.Data.SQLite.SQLiteConnection (Parameter 'connection')");
        }
    }
}
