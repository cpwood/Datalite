using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;
using Datalite.Destination;
using Microsoft.Data.Sqlite;

namespace Datalite.Testing
{
    /// <summary>
    /// Testing extension methods
    /// </summary>
    public static class SqliteConnectionTestingExtensions
    {
        /// <summary>
        /// Load the schema, indexes and data for a table so that unit tests can be performed upon them.
        /// </summary>
        /// <param name="connection"></param>
        /// <param name="table">The table name.</param>
        /// <returns></returns>
        public static Task<SqliteTable?> LoadTableAsync(this SqliteConnection connection, string table)
        {
            return LoadTableAsync((DbConnection)connection, table);
        }

        /// <summary>
        /// Load the schema, indexes and data for a table so that unit tests can be performed upon them.
        /// </summary>
        /// <param name="connection"></param>
        /// <param name="table">The table name.</param>
        /// <returns></returns>
        public static async Task<SqliteTable?> LoadTableAsync(DbConnection connection, string table)
        {
            var opened = false;

            try
            {
                if (connection.State != ConnectionState.Open)
                {
                    await connection.OpenAsync();
                    opened = true;
                }

                // Check the table exists
                var sql = @$"
                SELECT      1
                FROM        sqlite_master
                WHERE       type = 'table'
                AND         tbl_name = '{table}'";

                var cmd = connection.CreateCommand();
                cmd.CommandText = sql;
                var result = await cmd.ExecuteScalarAsync();

                if (result == null)
                    return null;

                var sqliteTable = new SqliteTable(table);

                // Discover the columns
                sql = $@"
                SELECT      lower(name) AS Name,
                            type AS StorageClass,
                            [notnull] AS Required
                FROM        pragma_table_info('{table}')";

                cmd = connection.CreateCommand();
                cmd.CommandText = sql;

                await using (var reader = await cmd.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        sqliteTable.Columns.Add(reader.GetString(0), new SqliteColumn(reader.GetString(0),
                            StoragesClasses.GetStorageClassTypeFromName(reader.GetString(1)), reader.GetBoolean(2)));
                    }
                }

                // Discover the indexes
                sql = $@"
                SELECT      i.name AS IndexName,
                            lower(ic.name) AS ColumnName,
                            ic.seqno + 1 AS ColumnOrder
                FROM        pragma_index_list('{table}') AS i
                CROSS JOIN  pragma_index_xinfo(i.name) AS ic
                WHERE       ic.name IS NOT NULL
                ORDER BY    ic.seqno";

                cmd = connection.CreateCommand();
                cmd.CommandText = sql;

                var indexes = new List<SqliteIndex>();

                await using (var reader = await cmd.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        indexes.Add(new SqliteIndex(reader.GetString(0), reader.GetString(1), reader.GetInt32(2)));
                    }
                }

                var output = new List<string[]>();
                var grouped = indexes.GroupBy(x => x.IndexName, x => x.ColumnName, (n, c) => new
                {
                    Name = n,
                    Columns = c.ToArray()
                }).ToArray();

                foreach (var g in grouped)
                {
                    if (g.Columns.Any() && !output.Any(x => x.SequenceEqual(g.Columns)))
                    {
                        output.Add(g.Columns);
                    }
                }

                sqliteTable.Indexes = output.ToArray();

                // Finally, get the data!
                sql = $@"
                SELECT      *
                FROM        {table}";

                cmd = connection.CreateCommand();
                cmd.CommandText = sql;

                var rows = new List<Dictionary<string, object>>();

                await using (var reader = await cmd.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        var values = new Dictionary<string, object>();

                        for (var i = 0; i < reader.FieldCount; i++)
                        {
                            var key = reader.GetName(i);
                            var val = reader.GetValue(i);

                            if (val != DBNull.Value) 
                                values.Add(key.ToLowerInvariant(), val);
                        }

                        rows.Add(values);
                    }
                }

                sqliteTable.Rows = rows.ToArray();


                return sqliteTable;
            }
            finally
            {
                if (opened)
                    await connection.CloseAsync();
            }
        }
    }
}
