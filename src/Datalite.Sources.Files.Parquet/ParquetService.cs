using System.Collections.Generic;
using System.IO.Abstractions;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Datalite.Destination;
using Parquet;

namespace Datalite.Sources.Files.Parquet
{
    internal class ParquetService
    {
        private readonly SqliteConnectionBroker _connection;
        private readonly IFileSystem _fileSystem;

        internal ParquetService(SqliteConnectionBroker connection, IFileSystem fileSystem)
        {
            _connection = connection;
            _fileSystem = fileSystem;
        }

        public async Task ExecuteAsync(ParquetDataliteContext context)
        {
            var opened = false;

            try
            {
                if (!_connection.IsOpen)
                {
                    await _connection.Connection.OpenAsync();
                    opened = true;
                }

                var tableDefinition = await DetermineTableDefinition(context);

                await _connection.CreateTableAsync(tableDefinition);

                await ReadData(context, tableDefinition);

                foreach (var index in context.Indexes)
                {
                    await _connection.CreateIndexAsync(context.TableName, index);
                }
            }
            finally
            {
                if (opened)
                    await _connection.Connection.CloseAsync();
            }
        }

        private async Task<TableDefinition> DetermineTableDefinition(ParquetDataliteContext context)
        {
            var tableDefinition = new TableDefinition(context.TableName);

            await using var fileStream = _fileSystem.File.OpenRead(context.Filename);
            using var parquetReader = new ParquetReader(fileStream);

            var dataFields = parquetReader.Schema.GetDataFields();

            foreach (var field in dataFields)
            {
                tableDefinition.Columns[field.Name] = new Column(field.Name, field.ClrType, !field.HasNulls);
            }

            return tableDefinition;
        }

        private async Task ReadData(ParquetDataliteContext context, TableDefinition tableDefinition)
        {
            var columns = tableDefinition.Columns.Values.Select(x => x.Name).ToArray();
            var builder = new StringBuilder();
            var header = $"INSERT INTO {tableDefinition.Name} ({string.Join(',', columns)}) VALUES";
            var values = new List<string>();
            var valueCount = 0;

            builder.AppendLine(header);

            await using var fileStream = _fileSystem.File.OpenRead(context.Filename);
            using var parquetReader = new ParquetReader(fileStream);

            for (var i = 0; i < parquetReader.RowGroupCount; i++)
            {
                var parquetColumns = parquetReader.ReadEntireRowGroup(i);
                var rows = parquetColumns[0].Data.LongLength;

                for (long rowIndex = 0; rowIndex < rows; rowIndex++)
                {
                    values.Clear();

                    if (valueCount == 1000)
                    {
                        await _connection.RunSqlFromBuilderAsync(builder);

                        builder.Clear();
                        builder.AppendLine(header);
                        valueCount = 0;
                    }

                    foreach (var column in parquetColumns)
                    {

                        values.Add(column.Data?.GetValue(rowIndex) == null
                            ? "NULL"
                            : column.Data.GetValue(rowIndex).Convert(column.Field.ClrType,
                                tableDefinition.Columns[column.Field.Name].StorageClass));
                    }

                    builder.AppendLine($"({string.Join(',', values)}),");
                    valueCount++;
                }


            }

            if (valueCount > 0)
                await _connection.RunSqlFromBuilderAsync(builder);
        }
    }
}
