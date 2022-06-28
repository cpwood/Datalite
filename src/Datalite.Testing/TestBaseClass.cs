using System;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;

namespace Datalite.Testing
{
    /// <summary>
    /// Base class for all in-memory Sqlite tests.
    /// </summary>
    public abstract class TestBaseClass
    {
        /// <summary>
        /// Creates an in-memory Sqlite database with an open connection. All data is lost when
        /// the connection is closed.
        /// </summary>
        /// <param name="action">The work to be performed with the in-memory database connection.</param>
        /// <returns></returns>
        public async Task WithSqliteInMemoryConnection(Func<SqliteConnection, Task> action)
        {
            await using var connection = new SqliteConnection("Data Source=:memory:");
            await connection.OpenAsync();
            await action(connection);
        }
    }
}
