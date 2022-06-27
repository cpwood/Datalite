using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Datalite.Destination;

namespace Datalite.Sources.Databases.AzureTables
{
    internal class AzureTableService
    {
        private readonly SqliteConnectionBroker _sqliteConnection;
        private readonly IAzureTableClient _tableClient;

        // Don't include these columns in the output table, either because we're already
        // accounting for them or we're just not interested.
        private readonly string[] _ignoredColumns = { "odata.etag", "PartitionKey", "RowKey", "Timestamp" };

        // We will always apply these indexes, regardless of user configuration.
        private readonly string[][] _defaultIndexes =
        {
            new[] { "PartitionKey" },
            new[] { "PartitionKey", "RowKey" }
        };

        internal AzureTableService(
            SqliteConnectionBroker sqliteConnection,
            IAzureTableClient tableClient)
        {
            _sqliteConnection = sqliteConnection;
            _tableClient = tableClient;
        }

        public async Task ExecuteAsync(AzureTablesDataliteContext context)
        {
            var opened = false;

            try
            {
                if (!_sqliteConnection.IsOpen)
                {
                    await _sqliteConnection.Connection.OpenAsync();
                    opened = true;
                }

                switch (context.Mode)
                {
                    case AzureTablesDataliteContext.CommandType.Tables:
                        await ExecuteTablesAsync(context.Tables);
                        break;
                    case AzureTablesDataliteContext.CommandType.Table:
                        await ExecuteTableAsync(context.Table, context.OutputTable, context.TableDefinition,
                            context.Indexes.ToArray(), context.Filter);
                        break;
                }
            }
            finally
            {
                if (opened)
                    await _sqliteConnection.Connection.CloseAsync();
            }
        }

        public async Task ExecuteTablesAsync(string[] tables)
        {
            var finalTables = new List<string>();
            finalTables.AddRange(tables);

            if (!finalTables.Any())
            {
                var remoteTables = _tableClient.QueryTablesAsync();

                await foreach (var remoteTable in remoteTables)
                {
                    finalTables.Add(remoteTable.Name);
                }
            }

            foreach (var table in finalTables)
            {
                await ExecuteTableAsync(table, table, null, Array.Empty<string[]>(), null);
            }
        }

        public async Task ExecuteTableAsync(
            string table,
            string? outputTable,
            TableDefinition? tableDefinition,
            string[][] indexes,
            string? filter
            )
        {
            if (string.IsNullOrEmpty(outputTable))
                outputTable = table;

            tableDefinition ??= await DetermineTableDefinition(table, outputTable, filter);

            await _sqliteConnection.CreateTableAsync(tableDefinition);

            await DownloadRecordsAsync(table, tableDefinition, filter);

            foreach (var index in _defaultIndexes)
            {
                await _sqliteConnection.CreateIndexAsync(table, index);
            }

            foreach (var index in indexes)
            {
                if (!_defaultIndexes.Any(x => x.SequenceEqual(index)))
                    await _sqliteConnection.CreateIndexAsync(table, index);
            }
        }

        private async Task<TableDefinition> DetermineTableDefinition(string table, string outputTable, string? filter)
        {
            var count = 0;

            var items = _tableClient.QueryRecordsAsync(table, filter);

            var columns = new Dictionary<string, Column>
        {
            { "PartitionKey", new Column("PartitionKey", typeof(string), true) },
            { "RowKey", new Column("RowKey", typeof(string), true) },
            { "Timestamp", new Column("Timestamp", typeof(DateTimeOffset), false) }
        };

            await foreach (var item in items)
            {
                if (item == null) continue;

                foreach (var key in item.Keys.Where(x => !_ignoredColumns.Contains(x)))
                {
                    if (!columns.ContainsKey(key) && item.TryGetValue(key, out var value) && value != null)
                    {
                        columns[key] = new Column(key, value.GetType(), false);
                    }
                }

                count++;

                if (count > 500)
                    break;
            }

            return new TableDefinition(outputTable)
            {
                Columns = columns
            };
        }

        private async Task DownloadRecordsAsync(string table, TableDefinition tableDefinition, string? filter)
        {
            var columns = tableDefinition.Columns.Values.Select(x => x.Name).ToArray();
            var builder = new StringBuilder();
            var header = $"INSERT INTO {tableDefinition.Name} ({string.Join(',', columns)}) VALUES";
            var values = new List<string>();
            var valueCount = 0;

            builder.AppendLine(header);

            var items = _tableClient.QueryRecordsAsync(table, filter);

            await foreach (var item in items)
            {
                values.Clear();

                if (valueCount == 1000)
                {
                    await _sqliteConnection.RunSqlFromBuilderAsync(builder);

                    builder.Clear();
                    builder.AppendLine(header);
                    valueCount = 0;
                }

                values.Add(item.PartitionKey.As(tableDefinition.Columns["PartitionKey"].StorageClass));
                values.Add(item.RowKey.As(tableDefinition.Columns["RowKey"].StorageClass));
                values.Add(item.Timestamp.HasValue
                    ? item.Timestamp.Value.As(tableDefinition.Columns["Timestamp"].StorageClass)
                    : "NULL");

                foreach (var column in tableDefinition.Columns.Values.Where(x => !_ignoredColumns.Contains(x.Name)))
                {
                    if (item.TryGetValue(column.Name, out var value) && value != null)
                    {
                        values.Add(value.Convert(column.Type, column.StorageClass));
                    }
                    else
                    {
                        values.Add("NULL");
                    }
                }

                builder.AppendLine($"({string.Join(',', values)}),");
                valueCount++;
            }

            if (valueCount > 0)
                await _sqliteConnection.RunSqlFromBuilderAsync(builder);
        }
    }
}
