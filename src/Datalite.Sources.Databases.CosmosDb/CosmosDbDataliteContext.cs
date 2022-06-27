using System;
using System.Threading.Tasks;
using Datalite.Destination;

namespace Datalite.Sources.Databases.CosmosDb
{
    internal class CosmosDbDataliteContext : DataliteContext
    {
        private string _outputTable = string.Empty;

        public string Sql { get; set; } = string.Empty;

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
        public CosmosDbDataliteContext(Func<CosmosDbDataliteContext, Task> executionFunction)
            : base(c => executionFunction((CosmosDbDataliteContext)c))
        {
        }
    }
}
