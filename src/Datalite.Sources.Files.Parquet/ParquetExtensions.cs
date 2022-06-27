using System.IO;
using System.IO.Abstractions;
using Datalite.Exceptions;

namespace Datalite.Sources.Files.Parquet
{
    public static class ParquetExtensions
    {
        public static ParquetCommand FromParquet(this AddDataCommand adc, string filename)
        {
            if (string.IsNullOrEmpty(filename))
                throw new DataliteException("The path to a Parquet file must be provided.");

            var f = new FileInfo(filename);

            return adc.FromParquet(filename, f.Name.Replace(f.Extension, string.Empty));
        }

        public static ParquetCommand FromParquet(this AddDataCommand adc, string filename, string tableName)
        {
            return adc.FromParquet(filename, tableName, new FileSystem());
        }

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