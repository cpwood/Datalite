using System.IO;
using System.IO.Abstractions;
using Datalite.Exceptions;

namespace Datalite.Sources.Files.Csv
{
    public static class CsvExtensions
    {
        public static CsvCommand FromCsv(this AddDataCommand adc, string filename)
        {
            if (string.IsNullOrEmpty(filename))
                throw new DataliteException("The path to a CSV file must be provided.");

            var f = new FileInfo(filename);

            return adc.FromCsv(filename, f.Name.Replace(f.Extension, string.Empty));
        }

        public static CsvCommand FromCsv(this AddDataCommand adc, string filename, string tableName)
        {
            return adc.FromCsv(filename, tableName, new FileSystem());
        }

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