using System;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;

namespace Datalite.Testing
{
    public abstract class TestBaseClass
    {
        public async Task WithSqliteInMemoryConnection(Func<SqliteConnection, Task> action)
        {
            await using var connection = new SqliteConnection("Data Source=:memory:");
            await connection.OpenAsync();
            await action(connection);
        }
    }
}
