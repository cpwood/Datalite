using System.Threading.Tasks;
using Datalite.Destination;

namespace Datalite.Sources.Databases.AzureTables
{
    public class AzureSingleTableCommand
    {
        private readonly AzureTablesDataliteContext _context;

        internal AzureSingleTableCommand(AzureTablesDataliteContext context)
        {
            _context = context;
        }

        /// <summary>
        /// Configures the output table name. If this isn't configured, the output table
        /// name will be the same as the input table name.
        /// </summary>
        /// <param name="tableName"></param>
        /// <returns></returns>
        public AzureSingleTableCommand ToTable(string tableName)
        {
            _context.OutputTable = tableName;

            if (_context.TableDefinition != null)
                _context.TableDefinition.Name = tableName;

            return this;
        }

        /// <summary>
        /// Use the provided <paramref name="columns"/> in the output table.
        /// </summary>
        /// <param name="columns">The columns that the output table will contain.</param>
        /// <returns></returns>
        public AzureSingleTableCommand WithColumns(params Column[] columns)
        {
            _context.TableDefinition = new TableDefinition(_context.OutputTable ?? _context.Table);

            foreach (var column in columns)
            {
                _context.TableDefinition.Columns[column.Name] = column;
            }

            return this;
        }

        /// <summary>
        /// Filter the content of a table using a filter string. The string syntax is described
        /// at <see href="https://docs.microsoft.com/en-us/visualstudio/azure/vs-azure-tools-table-designer-construct-filter-strings?view=vs-2022">this page</see>.
        /// </summary>
        /// <param name="filter">The filter string.</param>
        /// <returns></returns>
        public AzureSingleTableCommand WithFilter(string filter)
        {
            _context.Filter = filter;
            return this;
        }

        /// <summary>
        /// Add an individual index that covers all the specified column names.
        /// </summary>
        /// <param name="columns">The columns to be included in this individual index.</param>
        /// <returns></returns>
        public AzureSingleTableCommand AddIndex(params string[] columns)
        {
            _context.Indexes.Add(columns);
            return this;
        }

        /// <summary>
        /// Run the data migration.
        /// </summary>
        /// <returns></returns>
        public Task ExecuteAsync()
        {
            return _context.ExecuteAsync();
        }
    }
}