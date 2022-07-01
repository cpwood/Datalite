using System.Data.SQLite;
using Datalite.Testing;

namespace Datalite.Sources.Databases.Sqlite.Tests.Integration
{
    // ReSharper disable once InconsistentNaming
    internal static class SQLiteConnectionExtensions
    {
        /// <summary>
        /// Load the schema, indexes and data for a table so that unit tests can be performed upon them.
        /// </summary>
        /// <param name="connection"></param>
        /// <param name="table">The table name.</param>
        /// <returns></returns>
        public static Task<SqliteTable?> LoadTableAsync(this SQLiteConnection connection, string table)
        {
            return SqliteConnectionTestingExtensions.LoadTableAsync(connection, table);
        }
    }
}
