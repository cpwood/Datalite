using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Datalite.Sources;
using Microsoft.Data.Sqlite;

namespace Datalite.Destination
{
    // ReSharper disable once InvalidXmlDocComment
    /// <summary>
    /// Acts as a broker to either a <see cref="SqliteConnection"/> object or a <see cref="System.Data.SQLite.SQLiteConnection"/> object.
    /// </summary>
    public class SqliteConnectionBroker
    {
        /// <summary>
        /// The Sqlite database connection.
        /// </summary>
        public DbConnection Connection { get; }

        /// <summary>
        /// Whether the Sqlite connection is currently open.
        /// </summary>
        public bool IsOpen => Connection.State == ConnectionState.Open;

        internal SqliteConnectionBroker(DbConnection connection)
        {
            if (connection.GetType() != typeof(SqliteConnection) &&
                connection.GetType().FullName != "System.Data.SQLite.SQLiteConnection")
            {
                throw new ArgumentOutOfRangeException(nameof(connection),
                    "The connection must either be a Microsoft.Data.Sqlite.SqliteConnection or a System.Data.SQLite.SQLiteConnection");
            }

            Connection = connection;
        }

        /// <summary>
        /// Create a table in the Sqlite database using the provided <see cref="TableDefinition" /> instance.
        /// </summary>
        /// <param name="tableDefinition">The table columns and name.</param>
        /// <returns></returns>
        public async Task CreateTableAsync(TableDefinition tableDefinition)
        {
            await using var cmd = Connection.CreateCommand();
            cmd.CommandText = tableDefinition.ToString();
            await cmd.ExecuteNonQueryAsync();
        }

        /// <summary>
        /// Create an individual index on the <paramref name="table"/> that covers the <paramref name="columns"/>.
        /// </summary>
        /// <param name="table">The name of the table to which the index will be applied.</param>
        /// <param name="columns">The columns that the index will cover.</param>
        /// <returns></returns>
        public async Task CreateIndexAsync(string table, string[] columns)
        {
            var sql =
                $"CREATE INDEX IF NOT EXISTS [IX_{table}_{string.Join('_', columns)}] ON {table} ({string.Join(',', columns.Select(x => $"[{x}]"))});";

            await using var cmd = Connection.CreateCommand();
            cmd.CommandText = sql;
            await cmd.ExecuteNonQueryAsync();
        }

        /// <summary>
        /// Moves data from an <see cref="IDataReader"/> instance into the Sqlite table defined by the <paramref name="tableDefinition"/>
        /// </summary>
        /// <param name="tableDefinition">The table columns and name.</param>
        /// <param name="reader">The <see cref="IDataReader"/> from which the data will be read.</param>
        /// <returns></returns>
        public async Task FromDataReaderAsync(
            TableDefinition tableDefinition,
            IDataReader reader)
        {
            var columns = tableDefinition.Columns.Values.Select(x => $"[{x.Name}]").ToArray();
            var builder = new StringBuilder();
            var header = $"INSERT INTO [{tableDefinition.Name}] ({string.Join(',', columns)}) VALUES";
            var values = new List<string>();
            var valueCount = 0;

            builder.AppendLine(header);

            while (reader.Read())
            {
                values.Clear();

                if (valueCount == 1000)
                {
                    await RunSqlFromBuilderAsync(builder);

                    builder.Clear();
                    builder.AppendLine(header);
                    valueCount = 0;
                }

                foreach (var column in tableDefinition.Columns.Values)
                {
                    values.Add(!reader.IsDBNull(reader.GetOrdinal(column.Name)) ? reader.GetSqlValue(column) : "NULL");
                }

                builder.AppendLine($"({string.Join(',', values)}),");
                valueCount++;
            }

            if (valueCount > 0)
                await RunSqlFromBuilderAsync(builder);
        }

        /// <summary>
        /// Runs the SQL defined in the <see cref="StringBuilder"/> instance, <paramref name="builder"/>.
        /// </summary>
        /// <param name="builder">The <see cref="StringBuilder"/> containing the SQL to execute.</param>
        /// <returns></returns>
        public async Task RunSqlFromBuilderAsync(StringBuilder builder)
        {
            var sql = builder.ToString().TrimEnd().TrimEnd(',');

            await using var cmd = Connection.CreateCommand();
            cmd.CommandText = sql;
            await cmd.ExecuteNonQueryAsync();
        }
    }
}
