using System.IO;
using System.IO.Abstractions;
using Datalite.Exceptions;

namespace Datalite.Sources.Files.Parquet
{
    /// <summary>
    /// Parquet Extensions
    /// </summary>
    public static class ParquetExtensions
    {
        /// <summary>
        /// Load data from a Parquet file. The destination table name is taken from the filename
        /// minus its .parquet extension.
        /// </summary>
        /// <param name="adc"></param>
        /// <param name="filename">The path to the Parquet file.</param>
        /// <returns></returns>
        /// <exception cref="DataliteException"></exception>
        public static ParquetCommand FromParquet(this AddDataCommand adc, string filename)
        {
            if (string.IsNullOrEmpty(filename))
                throw new DataliteException("The path to a Parquet file must be provided.");

            var f = new FileInfo(filename);

            return adc.FromParquet(filename, f.Name.Replace(f.Extension, string.Empty));
        }

        /// <summary>
        /// Load data from a Parquet file into a table with the given name.
        /// </summary>
        /// <param name="adc"></param>
        /// <param name="filename">The path to the Parquet file.</param>
        /// <param name="tableName">The Sqlite table name.</param>
        /// <returns></returns>
        public static ParquetCommand FromParquet(this AddDataCommand adc, string filename, string tableName)
        {
            return adc.FromParquet(filename, tableName, new FileSystem());
        }


        /// <summary>
        /// Load data from a Parquet file into a table with the given name.
        /// </summary>
        /// <param name="adc"></param>
        /// <param name="filename">The path to the Parquet file.</param>
        /// <param name="tableName">The Sqlite table name.</param>
        /// <param name="fileSystem">The filesystem to use. This could be something other than the local file system.</param>
        /// <returns></returns>
        /// <exception cref="DataliteException"></exception>
        public static ParquetCommand FromParquet(this AddDataCommand adc, string filename, string tableName, IFileSystem fileSystem)
        {
            if (string.IsNullOrEmpty(filename))
                throw new DataliteException("The path to a Parquet file must be provided.");

            if (string.IsNullOrEmpty(tableName))
                throw new DataliteException("An output table name must be provided.");

            var service = new ParquetService(adc.Connection, fileSystem);
            var context = new ParquetDataliteContext(filename, tableName, ctx => service.ExecuteAsync(ctx));


            return new ParquetCommand(context);
        }
    }
}