using System;
using System.IO;
using System.IO.Abstractions;
using System.Linq;
using System.Threading.Tasks;
using Datalite.Destination;
using Datalite.Sources.Files.Json;
using Newtonsoft.Json;

namespace Datalite.Sources.Objects
{
    internal class ObjectsService
    {
        private readonly SqliteConnectionBroker _connection;
        private readonly IFileSystem _fs;

        internal ObjectsService(
            SqliteConnectionBroker connection,
            IFileSystem fs)
        {
            _connection = connection;
            _fs = fs;
        }

        public async Task ExecuteAsync(ObjectsDataliteContext context)
        {
            var opened = false;
            var filename = _fs.Path.Combine(_fs.Path.GetTempPath(), $"{Guid.NewGuid()}.json");

            try
            {
                if (!_connection.IsOpen)
                {
                    await _connection.Connection.OpenAsync();
                    opened = true;
                }

                await using (var s = _fs.File.OpenWrite(filename))
                await using (var writer = new StreamWriter(s))
                using (var jsonWriter = new JsonTextWriter(writer))
                {
                    var ser = new JsonSerializer
                    {
                        Formatting = Formatting.None
                    };

                    ser.Serialize(jsonWriter, context.Objects);
                    await jsonWriter.FlushAsync();
                }

                var jsonBuilder = _connection
                    .Connection
                    .Add()
                    .FromJson(filename, context.TableName, false, _fs);

                if (context.TableDefinition != null)
                    jsonBuilder.WithColumns(context.TableDefinition.Columns.Values.ToArray());

                if (!context.SerializeNested)
                    jsonBuilder.IgnoreNested();

                foreach (var index in context.Indexes)
                {
                    jsonBuilder.AddIndex(index);
                }

                await jsonBuilder.ExecuteAsync();
            }
            finally
            {
                if (opened)
                    await _connection.Connection.CloseAsync();

                if (_fs.File.Exists(filename))
                    _fs.File.Delete(filename);
            }
        }
    }
}
