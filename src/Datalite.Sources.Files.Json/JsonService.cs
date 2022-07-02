using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Abstractions;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Datalite.Destination;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Datalite.Sources.Files.Json
{
    internal class JsonService
    {
        private readonly SqliteConnectionBroker _connection;
        private readonly IFileSystem _fs;

        public JsonService(SqliteConnectionBroker connection, IFileSystem fs)
        {
            _connection = connection;
            _fs = fs;
        }


        public async Task ExecuteAsync(JsonDataliteContext context)
        {
            var opened = false;

            try
            {
                if (!_connection.IsOpen)
                {
                    await _connection.Connection.OpenAsync();
                    opened = true;
                }

                var tableDefinition = context.TableDefinition ?? await DetermineTableDefinition(
                    context.Filename,
                    context.Jsonl,
                    context.SerializeNested,
                    context.OutputTable);

                if (!tableDefinition.Columns.Any())
                    return;

                await _connection.CreateTableAsync(tableDefinition);

                await ReadJson(
                    context.Filename,
                    context.SerializeNested,
                    context.Jsonl,
                    tableDefinition);

                foreach (var index in context.Indexes)
                {
                    await _connection.CreateIndexAsync(context.OutputTable, index);
                }
            }
            finally
            {
                if (opened)
                    await _connection.Connection.CloseAsync();
            }
        }

        private async Task<TableDefinition> DetermineTableDefinition(
            string filename, 
            bool jsonl,
            bool serializeNested,
            string tableName)
        {
            var columns = new Dictionary<string, Column>();
            var columnCounts = new Dictionary<string, long>();

            await using var stream = _fs.File.OpenRead(filename);
            using var reader = new StreamReader(stream);
            using var jsonReader = new JsonTextReader(reader);
            jsonReader.SupportMultipleContent = jsonl;

            var inRootObject = false;
            var records = 0;

            var propertyName = string.Empty;

            while (await jsonReader.ReadAsync() && records < 500)
            {
                switch (jsonReader.TokenType)
                {
                    case JsonToken.StartObject:
                        if (inRootObject)
                        {
                            // Read the whole object to skip ahead
                            await JObject.LoadAsync(jsonReader);

                            if (serializeNested)
                                this.HandleProperty(propertyName, typeof(string), columns, columnCounts);
                        }
                        else if (!inRootObject)
                        {
                            inRootObject = true;
                            records++;
                        }
                        break;
                    case JsonToken.EndObject:
                        inRootObject = false;
                        break;
                    case JsonToken.StartArray:
                        if (inRootObject)
                        {
                            // Read the whole array to skip ahead
                            await JArray.LoadAsync(jsonReader);

                            if (serializeNested)
                                this.HandleProperty(propertyName, typeof(string), columns, columnCounts);
                        }
                        break;
                    case JsonToken.PropertyName:
                        propertyName = jsonReader.Value as string;
                        break;
                    case JsonToken.Boolean:
                        this.HandleProperty(propertyName, typeof(bool), columns, columnCounts);
                        break;
                    case JsonToken.Bytes:
                        this.HandleProperty(propertyName, typeof(byte[]), columns, columnCounts);
                        break;
                    case JsonToken.Date:
                        this.HandleProperty(propertyName, typeof(DateTime), columns, columnCounts);
                        break;
                    case JsonToken.Float:
                        this.HandleProperty(propertyName, typeof(float), columns, columnCounts);
                        break;
                    case JsonToken.Integer:
                        this.HandleProperty(propertyName, typeof(long), columns, columnCounts);
                        break;
                    case JsonToken.Null:
                        this.HandleProperty(propertyName, typeof(UnknownDataType), columns, columnCounts);
                        break;
                    case JsonToken.String:
                        this.HandleProperty(propertyName, typeof(string), columns, columnCounts);
                        break;
                }
            }

            foreach (var column in columns)
            {
                if (column.Value.Type == typeof(UnknownDataType))
                    columns[column.Key] = new Column(column.Key, typeof(int), false);

                if (columnCounts[column.Key] != records)
                    columns[column.Key] = new Column(column.Key, column.Value.Type, false);
            }

            var definition = new TableDefinition(tableName)
            {
                Columns = columns
            };

            return definition;
        }

        private void HandleProperty(string? name,
            Type type,
            Dictionary<string, Column> columns,
            Dictionary<string, long> columnCounts)
        {
            if (string.IsNullOrEmpty(name)) return;

            if (columns.ContainsKey(name))
            {
                if (type == typeof(UnknownDataType) && columns[name].Type != typeof(object))
                {
                    var existing = columns[name];
                    columns[name] = new Column(name, existing.Type, false);
                }
            }
            else
            {
                columns[name] = new Column(name, type, type != typeof(UnknownDataType));
            }

            columnCounts[name] = columnCounts.ContainsKey(name) ? columnCounts[name]++ : 1;
        }

        private async Task ReadJson(string filename, bool serializeNested, bool jsonl, TableDefinition tableDefinition)
        {
            var columns = tableDefinition.Columns;
            var builder = new StringBuilder();
            var header = $"INSERT INTO {tableDefinition.Name} ({string.Join(',', columns.Keys)}) VALUES";
            var valueCount = 0;

            builder.AppendLine(header);

            await using var stream = _fs.File.OpenRead(filename);
            using var reader = new StreamReader(stream);
            using var jsonReader = new JsonTextReader(reader);
            jsonReader.SupportMultipleContent = jsonl;

            var rowValues = new Dictionary<string, string>();

            var propertyName = string.Empty;
            var inRootObject = false;

            while (await jsonReader.ReadAsync())
            {
                switch (jsonReader.TokenType)
                {
                    case JsonToken.StartObject:
                        if (inRootObject)
                        {
                            var obj = await JObject.LoadAsync(jsonReader);
                            if (propertyName != null && serializeNested)
                                rowValues[propertyName] = $"'{obj.ToString(Formatting.None).Replace("'", "''")}'";
                        }
                        else if (!inRootObject)
                        {
                            inRootObject = true;
                        }
                        break;
                    case JsonToken.EndObject:
                        inRootObject = false;

                        valueCount++;

                        var orderedValues = new List<string>();

                        foreach (var definitionColumn in tableDefinition.Columns.Values)
                        {
                            orderedValues.Add(rowValues.ContainsKey(definitionColumn.Name)
                                ? rowValues[definitionColumn.Name]
                                : "NULL");
                        }

                        builder.AppendLine($"({string.Join(',', orderedValues)}),");
                        rowValues.Clear();

                        if (valueCount == 1000)
                        {
                            await _connection.RunSqlFromBuilderAsync(builder);

                            builder.Clear();
                            builder.AppendLine(header);
                            valueCount = 0;
                        }
                        break;
                    case JsonToken.StartArray:
                        if (inRootObject)
                        {
                            var arr = await JArray.LoadAsync(jsonReader);
                            if (!string.IsNullOrEmpty(propertyName) && serializeNested)
                                rowValues[propertyName] = $"'{arr.ToString(Formatting.None).Replace("'", "''")}'";
                        }
                        break;
                    case JsonToken.PropertyName:
                        propertyName = jsonReader.Value as string;
                        break;
                    case JsonToken.Boolean:
                    case JsonToken.Bytes:
                    case JsonToken.Date:
                    case JsonToken.Float:
                    case JsonToken.Integer:
                    case JsonToken.Null:
                    case JsonToken.String:
                        if (!string.IsNullOrEmpty(propertyName) && tableDefinition.Columns.ContainsKey(propertyName))
                            rowValues[propertyName] = jsonReader.GetSqlValue(tableDefinition.Columns[propertyName]);
                        break;
                }
            }

            if (valueCount > 0)
                await _connection.RunSqlFromBuilderAsync(builder);
        }
    }
}
