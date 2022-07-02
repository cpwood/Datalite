using System;
using System.Data.SqlClient;
using Datalite.Destination;
using FluentAssertions;
using Xunit;

namespace Datalite.Tests.Destination
{
    public class SqliteConnectionBrokerTests
    {
        [Fact]
        public void AddOnNonSqliteDatabaseRejected()
        {
            // This is a SQL Server connection.
            var connection = new SqlConnection();
            var action = () => new SqliteConnectionBroker(connection);
            action
                .Should()
                .Throw<ArgumentOutOfRangeException>()
                .WithMessage(
                    "The connection must either be a Microsoft.Data.Sqlite.SqliteConnection or a System.Data.SQLite.SQLiteConnection (Parameter 'connection')");
        }
    }
}
