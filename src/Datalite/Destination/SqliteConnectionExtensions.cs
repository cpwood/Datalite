using System;
using System.Data.Common;
using System.Threading.Tasks;
using Datalite.Sources;
using Microsoft.Data.Sqlite;

namespace Datalite.Destination
{
    /// <summary>
    /// Extensions for <see cref="SqliteConnection" />.
    /// </summary>
    public static class SqliteConnectionExtensions
    {
        /// <summary>
        /// Add data to a Sqlite database.
        /// </summary>
        /// <param name="connection"></param>
        /// <returns></returns>
        public static AddDataCommand Add(this SqliteConnection connection)
        {
            return new AddDataCommand(new SqliteConnectionBroker(connection));
        }

        // ReSharper disable once InvalidXmlDocComment

#pragma warning disable CS1574 // XML comment has cref attribute that could not be resolved
        /// <summary>
        /// Add data to a Sqlite database.
        /// </summary>
        /// <param name="connection">Must be a <see cref="SqliteConnection"/> object or a <see cref="System.Data.SQLite.SQLiteConnection"/> object.</param>
        /// <returns></returns>
        /// <exception cref="ArgumentOutOfRangeException"></exception>
        public static AddDataCommand Add(this DbConnection connection)
#pragma warning restore CS1574 // XML comment has cref attribute that could not be resolved
        {
            if (connection.GetType() != typeof(SqliteConnection) &&
                connection.GetType().FullName != "System.Data.SQLite.SQLiteConnection")
            {
                throw new ArgumentOutOfRangeException(nameof(connection),
                    "The connection must either be a Microsoft.Data.Sqlite.SqliteConnection or a System.Data.SQLite.SQLiteConnection");
            }

            return new AddDataCommand(new SqliteConnectionBroker(connection));
        }
        
        /// <summary>
        /// Drop the table with the given name.
        /// </summary>
        /// <param name="connection"></param>
        /// <param name="table">The table name.</param>
        /// <returns></returns>
        public static async Task DropTableAsync(this SqliteConnection connection, string table)
        {
            var sql = $"DROP TABLE [{table}];";
            await using var cmd = new SqliteCommand(sql, connection);
            await cmd.ExecuteNonQueryAsync();
        }

        /// <summary>
        /// Shrinks the Sqlite database.
        /// </summary>
        /// <param name="connection"></param>
        /// <returns></returns>
        public static async Task VacuumAsync(this SqliteConnection connection)
        {
            await using var cmd = new SqliteCommand("VACUUM;", connection);
            await cmd.ExecuteNonQueryAsync();
        }
    }
}