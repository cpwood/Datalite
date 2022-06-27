using System;
using System.Threading.Tasks;
using CsvHelper.Configuration;
using Datalite.Destination;

namespace Datalite.Sources.Files.Csv
{
    internal class CsvDataliteContext : DataliteContext
    {
        private string _outputTable = string.Empty;

        public string Filename { get; }

        public string OutputTable
        {
            get => _outputTable;
            set
            {
                _outputTable = value;

                if (TableDefinition != null)
                    TableDefinition.Name = value;
            }
        }

        public TableDefinition? TableDefinition { get; set; }
        public Action<CsvConfiguration> Configuration { get; set; } = _ => { };

        public CsvDataliteContext(
            string filename,
            string outputTable,
            Func<CsvDataliteContext, Task> executionFunction)
            : base(c => executionFunction((CsvDataliteContext)c))
        {
            Filename = filename;
            OutputTable = outputTable;
        }
    }
}