using System.Data;
using System.Globalization;
using System.IO;
using System.IO.Abstractions;
using System.Text;
using System.Threading.Tasks;
using CsvHelper;
using CsvHelper.Configuration;
using Datalite.Destination;

namespace Datalite.Sources.Files.Csv
{
    internal class CsvService
    {
        private readonly SqliteConnectionBroker _connection;
        private readonly IFileSystem _fileSystem;

        internal CsvService(SqliteConnectionBroker connection, IFileSystem fileSystem)
        {
            _connection = connection;
            _fileSystem = fileSystem;
        }

        public async Task ExecuteAsync(CsvDataliteContext context)
        {
            var opened = false;

            try
            {
                if (!_connection.IsOpen)
                {
                    await _connection.Connection.OpenAsync();
                    opened = true;
                }

                var tableDefinition = context.TableDefinition ?? await DetermineTableDefinition(context);

                await _connection.CreateTableAsync(tableDefinition);

                var config = new CsvConfiguration(CultureInfo.InvariantCulture);
                context.Configuration(config);

                await using var stream = _fileSystem.File.OpenRead(context.Filename);
                using var reader = new StreamReader(stream);
                using var csv = new CsvReader(reader, config);
                using var dr = new CsvDataReader(csv);

                await _connection.FromDataReaderAsync(tableDefinition, dr);

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

        private async Task<TableDefinition> DetermineTableDefinition(CsvDataliteContext context)
        {
            var config = new CsvConfiguration(CultureInfo.InvariantCulture);
            context.Configuration(config);

            var buffer = new StringBuilder();

            // Limit the schema detection to the first 500 records
            // for memory reasons.
            await using (var stream = _fileSystem.File.OpenRead(context.Filename))
            using (var reader = new StreamReader(stream))
            {
                var lines = 0;

                while (!reader.EndOfStream && lines < 500)
                {
                    buffer.AppendLine(await reader.ReadLineAsync());
                    lines++;
                }
            }

            using var sr = new StringReader(buffer.ToString());
            using var csv = new CsvReader(sr, config);
            using var dr = new CsvDataReader(csv);

            var dt = new DataTable();
            dt.Load(dr);

            var definition = new TableDefinition(context.OutputTable);

            foreach (DataColumn column in dt.Columns)
            {
                definition.Columns[column.ColumnName] = new Column(column.ColumnName, column.DataType,
                    !column.AllowDBNull);
            }

            return definition;
        }
    }
}
