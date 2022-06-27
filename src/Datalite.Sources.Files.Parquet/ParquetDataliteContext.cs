using System;
using System.Threading.Tasks;

namespace Datalite.Sources.Files.Parquet
{
    internal class ParquetDataliteContext : DataliteContext
    {
        public string Filename { get; }
        public string TableName { get; }
  
        public ParquetDataliteContext(
            string filename,
            string tableName,
            Func<ParquetDataliteContext, Task> executionFunction)
            : base(c => executionFunction((ParquetDataliteContext)c))
        {
            Filename = filename;
            TableName = tableName;
        }
    }
}