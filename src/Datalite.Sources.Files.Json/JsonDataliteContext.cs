using System;
using System.Threading.Tasks;
using Datalite.Destination;

namespace Datalite.Sources.Files.Json
{
    internal class JsonDataliteContext : DataliteContext
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
        public bool SerializeNested { get; set; } = true;
        public bool Jsonl { get; }

        public JsonDataliteContext(
            string filename,
            string outputTable,
            bool jsonl,
            Func<JsonDataliteContext, Task> executionFunction)
            : base(c => executionFunction((JsonDataliteContext)c))
        {
            Filename = filename;
            OutputTable = outputTable;
            Jsonl = jsonl;
        }
    }
}
