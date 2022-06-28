using System.IO;
using System.IO.Abstractions;
using Datalite.Exceptions;

namespace Datalite.Sources.Files.Csv
{
    /// <summary>
    /// CSV Extensions
    /// </summary>
    public static class CsvExtensions
    {
        /// <summary>
        /// Load data from a CSV file. The destination table name is taken from the filename
        /// minus its .csv extension.
        /// </summary>
        /// <param name="adc"></param>
        /// <param name="filename">The path to the CSV file.</param>
        /// <returns></returns>
        /// <exception cref="DataliteException"></exception>
        public static CsvCommand FromCsv(this AddDataCommand adc, string filename)
        {
            if (string.IsNullOrEmpty(filename))
                throw new DataliteException("The path to a CSV file must be provided.");

            var f = new FileInfo(filename);

            return adc.FromCsv(filename, f.Name.Replace(f.Extension, string.Empty));
        }

        /// <summary>
        /// Load data from a CSV file into a table with the given name.
        /// </summary>
        /// <param name="adc"></param>
        /// <param name="filename">The path to the CSV file.</param>
        /// <param name="tableName">The Sqlite table name.</param>
        /// <returns></returns>
        public static CsvCommand FromCsv(this AddDataCommand adc, string filename, string tableName)
        {
            return adc.FromCsv(filename, tableName, new FileSystem());
        }

        /// <summary>
        /// Load data from a CSV file into a table with the given name.
        /// </summary>
        /// <param name="adc"></param>
        /// <param name="filename">The path to the CSV file.</param>
        /// <param name="tableName">The Sqlite table name.</param>
        /// <param name="fileSystem">The filesystem to use. This could be something other than the local file system.</param>
        /// <returns></returns>
        /// <exception cref="DataliteException"></exception>
        public static CsvCommand FromCsv(this AddDataCommand adc, string filename, string tableName, IFileSystem fileSystem)
        {
            if (string.IsNullOrEmpty(filename))
                throw new DataliteException("The path to a CSV file must be provided.");

            if (string.IsNullOrEmpty(tableName))
                throw new DataliteException("An output table name must be provided.");

            var service = new CsvService(adc.Connection, fileSystem);
            var context = new CsvDataliteContext(filename, tableName, ctx => service.ExecuteAsync(ctx));

            return new CsvCommand(context);
        }
    }
}