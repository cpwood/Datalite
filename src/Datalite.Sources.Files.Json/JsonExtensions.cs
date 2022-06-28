using System.IO;
using System.IO.Abstractions;
using Datalite.Exceptions;

namespace Datalite.Sources.Files.Json
{
    /// <summary>
    /// CSV Extensions
    /// </summary>
    public static class JsonExtensions
    {
        /// <summary>
        /// Load data from a JSON file. The destination table name is taken from the filename
        /// minus its .json extension.
        /// </summary>
        /// <param name="adc"></param>
        /// <param name="filename">The path to the JSON file.</param>
        /// <param name="jsonl">Whether the JSON file is written using JSONL notation.</param>
        /// <returns></returns>
        /// <exception cref="DataliteException"></exception>
        public static JsonCommand FromJson(this AddDataCommand adc, string filename, bool jsonl = false)
        {
            if (string.IsNullOrEmpty(filename))
                throw new DataliteException("The path to a JSON file must be provided.");

            var f = new FileInfo(filename);
            var tableName = f.Name.Replace(f.Extension, string.Empty);

            return adc.FromJson(filename, tableName, jsonl);
        }

        /// <summary>
        /// Load data from a JSON file into a table with the given name.
        /// </summary>
        /// <param name="adc"></param>
        /// <param name="filename">The path to the JSON file.</param>
        /// <param name="tableName">The Sqlite table name.</param>
        /// <param name="jsonl">Whether the JSON file is written using JSONL notation.</param>
        /// <returns></returns>
        public static JsonCommand FromJson(this AddDataCommand adc, string filename, string tableName, bool jsonl = false)
        {
            return adc.FromJson(filename, tableName, jsonl, new FileSystem());
        }

        /// <summary>
        /// Load data from a JSON file into a table with the given name.
        /// </summary>
        /// <param name="adc"></param>
        /// <param name="filename">The path to the JSON file.</param>
        /// <param name="tableName">The Sqlite table name.</param>
        /// <param name="jsonl">Whether the JSON file is written using JSONL notation.</param>
        /// <param name="fileSystem">The filesystem to use. This could be something other than the local file system.</param>
        /// <returns></returns>
        /// <exception cref="DataliteException"></exception>
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