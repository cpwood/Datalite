using System;
using System.Data.Common;
using System.Threading.Tasks;
using Datalite.Sources;
using Microsoft.Data.Sqlite;

namespace Datalite.Destination
{
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
        /// <summary>
        /// Add data to a Sqlite database.
        /// </summary>
        /// <param name="connection">Must be a <see cref="SqliteConnection"/> object or a <see cref="System.Data.SQLite.SQLiteConnection"/> object.</param>
        /// <returns></returns>
        /// <exception cref="ArgumentOutOfRangeException"></exception>
        public static AddDataCommand Add(this DbConnection connection)
        {
            if (connection.GetType() != typeof(SqliteConnection) &&
                connection.GetType().FullName != "System.Data.SQLite.SQLiteConnection")
            {
                throw new ArgumentOutOfRangeException(nameof(connection),
                    "The connection must either be a Microsoft.Data.Sqlite.SqliteConnection or a System.Data.SQLite.SQLiteConnection");
            }

            return new AddDataCommand(new SqliteConnectionBroker(connection));
        }

        //public static async Task CreateTableAsync(this SqliteConnection connection, TableDefinition tableDefinition)
        //{
        //    await using var cmd = new SqliteCommand(tableDefinition.ToString(), connection);
        //    await cmd.ExecuteNonQueryAsync();
        //}

        //public static async Task CreateIndexAsync(this SqliteConnection connection, string table, string[] columns)
        //{
        //    var sql =
        //        $"CREATE INDEX IF NOT EXISTS [IX_{string.Join('_', columns)}] ON {table} ({string.Join(',', columns.Select(x => $"[{x}]"))});";
        //    await using var cmd = new SqliteCommand(sql, connection);
        //    await cmd.ExecuteNonQueryAsync();
        //}

        //public static async Task FromDataReaderAsync(this SqliteConnection connection,
        //    TableDefinition tableDefinition,
        //    IDataReader reader)
        //{
        //    var columns = tableDefinition.Columns.Values.Select(x => $"[{x.Name}]").ToArray();
        //    var builder = new StringBuilder();
        //    var header = $"INSERT INTO [{tableDefinition.Name}] ({string.Join(',', columns)}) VALUES";
        //    var values = new List<string>();
        //    var valueCount = 0;

        //    builder.AppendLine(header);

        //    while (reader.Read())
        //    {
        //        values.Clear();

        //        if (valueCount == 1000)
        //        {
        //            await connection.AddRows(builder);

        //            builder.Clear();
        //            builder.AppendLine(header);
        //            valueCount = 0;
        //        }

        //        foreach (var column in tableDefinition.Columns.Values)
        //        {
        //            values.Add(!reader.IsDBNull(reader.GetOrdinal(column.Name)) ? reader.GetSqlValue(column) : "NULL");
        //        }

        //        builder.AppendLine($"({string.Join(',', values)}),");
        //        valueCount++;
        //    }

        //    if (valueCount > 0)
        //        await connection.AddRows(builder);
        //}

        //public static async Task AddRows(this SqliteConnection connection, StringBuilder builder)
        //{
        //    var sql = builder.ToString().TrimEnd().TrimEnd(',');
        //    await using var cmd = new SqliteCommand(sql, connection);
        //    await cmd.ExecuteNonQueryAsync();
        //}

        public static async Task DropTableAsync(this SqliteConnection connection, string table)
        {
            var sql = $"DROP TABLE [{table}];";
            await using var cmd = new SqliteCommand(sql, connection);
            await cmd.ExecuteNonQueryAsync();
        }

        public static async Task VacuumAsync(this SqliteConnection connection)
        {
            await using var cmd = new SqliteCommand("VACUUM;", connection);
            await cmd.ExecuteNonQueryAsync();
        }
    }
}