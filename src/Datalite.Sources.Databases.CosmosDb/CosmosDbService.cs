using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Datalite.Destination;
using Newtonsoft.Json.Linq;

namespace Datalite.Sources.Databases.CosmosDb
{
    internal class CosmosDbService
    {
        private readonly SqliteConnectionBroker _sqliteConnection;
        private readonly ICosmosDbClient _cosmosClient;

        // Don't include these values as columns in the output table.
        private readonly string[] _ignored = { "_rid", "_etag", "_self", "_attachments" };

        // These are types we will need to serialize prior to inclusion in the INSERT
        // statement, but the user may have elected to not serialize such types through
        // configuration.
        private readonly Type[] _nestedTypes = { typeof(JObject), typeof(JArray) };

        internal CosmosDbService(
            SqliteConnectionBroker sqliteConnection,
            ICosmosDbClient cosmosClient)
        {
            _sqliteConnection = sqliteConnection;
            _cosmosClient = cosmosClient;
        }

        public async Task ExecuteAsync(CosmosDbDataliteContext context)
        {
            var opened = false;

            try
            {
                if (!_sqliteConnection.IsOpen)
                {
                    await _sqliteConnection.Connection.OpenAsync();
                    opened = true;
                }

                var tableDefinition = context.TableDefinition ?? await DetermineTableDefinition(
                    context.Sql,
                    context.SerializeNested,
                    context.OutputTable);

                await _sqliteConnection.CreateTableAsync(tableDefinition);

                await RunAsync(context.Sql, tableDefinition);

                if (tableDefinition.Columns.Values.Any(x => x.Name == "id") &&
                    context.Indexes.Any(x => x.SequenceEqual(new[] { "id" })))
                    context.Indexes.Add(new[] { "id" });

                foreach (var index in context.Indexes)
                {
                    await _sqliteConnection.CreateIndexAsync(context.OutputTable, index);
                }
            }
            finally
            {
                if (opened)
                    await _sqliteConnection.Connection.CloseAsync();
            }
        }

        private async Task<TableDefinition> DetermineTableDefinition(
            string sql,
            bool serializeNested,
            string outputTable)
        {
            var count = 0;
            var columns = new Dictionary<string, Column>();

            using var queryResultSetIterator = _cosmosClient.GetItemQueryIterator(sql);

            while (queryResultSetIterator.HasMoreResults)
            {
                var currentResultSet = await queryResultSetIterator.ReadNextAsync();

                foreach (var result in currentResultSet)
                {
                    foreach (var key in result.Keys)
                    {
                        if (!columns.ContainsKey(key) && !_ignored.Contains(key))
                        {
                            // ReSharper disable once ConditionIsAlwaysTrueOrFalse
                            if (result[key] != null && (serializeNested || !_nestedTypes.Contains(result[key].GetType())))
                            {
                                columns[key] = new Column(key, result[key].GetType(), key == "id" || key == "_ts");
                            }
                        }
                    }

                    count++;

                    if (count > 500)
                        break;
                }

                if (count > 500)
                    break;
            }

            return new TableDefinition(outputTable)
            {
                Columns = columns
            };
        }

        private async Task RunAsync(
            string sql,
            TableDefinition tableDefinition)
        {
            var columns = tableDefinition.Columns.Values.Select(x => $"[{x.Name}]").ToArray();
            var builder = new StringBuilder();
            var header = $"INSERT INTO {tableDefinition.Name} ({string.Join(',', columns)}) VALUES";
            var values = new List<string>();
            var valueCount = 0;

            builder.AppendLine(header);

            using var queryResultSetIterator = _cosmosClient.GetItemQueryIterator(sql);

            while (queryResultSetIterator.HasMoreResults)
            {
                var currentResultSet = await queryResultSetIterator.ReadNextAsync();

                foreach (var result in currentResultSet)
                {
                    values.Clear();

                    if (valueCount == 1000)
                    {
                        await _sqliteConnection.RunSqlFromBuilderAsync(builder);

                        builder.Clear();
                        builder.AppendLine(header);
                        valueCount = 0;
                    }

                    foreach (var column in tableDefinition.Columns.Values)
                    {
                        // ReSharper disable once ConditionIsAlwaysTrueOrFalse
                        if (result.ContainsKey(column.Name) && result[column.Name] != null)
                        {
                            values.Add(result[column.Name].Convert(result[column.Name].GetType(), column.StorageClass));
                        }
                        else
                        {
                            values.Add("NULL");
                        }
                    }

                    builder.AppendLine($"({string.Join(',', values)}),");
                    valueCount++;
                }
            }

            if (valueCount > 0)
                await _sqliteConnection.RunSqlFromBuilderAsync(builder);
        }
    }
}
