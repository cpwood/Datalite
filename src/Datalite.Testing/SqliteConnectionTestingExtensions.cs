using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;
using Datalite.Destination;
using Microsoft.Data.Sqlite;

namespace Datalite.Testing
{
    public static class SqliteConnectionTestingExtensions
    {
        public static async Task<SqliteTable?> LoadTableAsync(this SqliteConnection connection, string table)
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

                var cmd = new SqliteCommand(sql, connection);
                var result = await cmd.ExecuteScalarAsync();

                if (result == null)
                    return null;

                var sqliteTable = new SqliteTable(table);

                // Discover the columns
                sql = $@"
                SELECT      name AS Name,
                            type AS StorageClass,
                            [notnull] AS Required
                FROM        pragma_table_info('{table}')";

                cmd = new SqliteCommand(sql, connection);

                await using (var reader = await cmd.ExecuteReaderAsync())
                {
                    while (reader.Read())
                    {
                        sqliteTable.Columns.Add(reader.GetString(0), new SqliteColumn(reader.GetString(0),
                            StoragesClasses.GetStorageClassTypeFromName(reader.GetString(1)), reader.GetBoolean(2)));
                    }
                }

                // Discover the indexes
                sql = $@"
                SELECT      i.name AS IndexName,
                            ic.name AS ColumnName,
                            ic.seqno + 1 AS ColumnOrder
                FROM        pragma_index_list('{table}') AS i
                CROSS JOIN  pragma_index_xinfo(i.name) AS ic
                WHERE       ic.name IS NOT NULL
                ORDER BY    ic.seqno";

                cmd = new SqliteCommand(sql, connection);

                var indexes = new List<SqliteIndex>();

                await using (var reader = await cmd.ExecuteReaderAsync())
                {
                    while (reader.Read())
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

                cmd = new SqliteCommand(sql, connection);

                var rows = new List<string[]>();
                var fields = new List<string>();

                await using (var reader = await cmd.ExecuteReaderAsync())
                {
                    while (reader.Read())
                    {
                        fields.Clear();

                        for (var i = 0; i < reader.FieldCount; i++)
                        {
                            var val = reader.GetValue(i);

                            fields.Add(val == DBNull.Value
                                ? "NULL"
                                : val.Convert(val.GetType(), sqliteTable.Columns.Values.ElementAt(i).StorageClass));
                        }

                        rows.Add(fields.ToArray());
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
