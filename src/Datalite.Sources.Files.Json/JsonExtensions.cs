using System.IO;
using System.IO.Abstractions;
using Datalite.Exceptions;

namespace Datalite.Sources.Files.Json
{
    public static class JsonExtensions
    {
        public static JsonCommand FromJson(this AddDataCommand adc, string filename, bool jsonl = false)
        {
            if (string.IsNullOrEmpty(filename))
                throw new DataliteException("The path to a JSON file must be provided.");

            var f = new FileInfo(filename);
            var tableName = f.Name.Replace(f.Extension, string.Empty);

            return adc.FromJson(filename, tableName, jsonl);
        }

        public static JsonCommand FromJson(this AddDataCommand adc, string filename, string tableName, bool jsonl = false)
        {
            return adc.FromJson(filename, tableName, jsonl, new FileSystem());
        }

        public static JsonCommand FromJson(this AddDataCommand adc, string filename, string tableName, bool jsonl, IFileSystem fileSystem)
        {
            if (string.IsNullOrEmpty(filename))
                throw new DataliteException("The path to a JSON file must be provided.");

            if (string.IsNullOrEmpty(tableName))
                throw new DataliteException("A valid output table name must be provided.");

            var service = new JsonService(adc.Connection, fileSystem);
            var context = new JsonDataliteContext(filename, tableName, jsonl,
                ctx => service.ExecuteAsync(ctx));

            return new JsonCommand(context);
        }
    }
}