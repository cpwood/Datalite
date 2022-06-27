using System;
using System.Threading.Tasks;
using Datalite.Destination;

namespace Datalite.Sources.Databases.AzureTables
{
    internal class AzureTablesDataliteContext : DataliteContext
    {
        private string _outputTable = string.Empty;

        public enum CommandType
        {
            Tables,
            Table
        }
        public CommandType Mode { get; set;}
        public TableDefinition? TableDefinition { get; set; }
        public string Table { get; set; } = string.Empty;
        public string[] Tables { get; set; } = Array.Empty<string>();

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

        public string? Filter { get; set; }

        public AzureTablesDataliteContext(
            Func<AzureTablesDataliteContext, Task> executionFunction)
            : base(c => executionFunction((AzureTablesDataliteContext)c))
        {
        }
    }
}
