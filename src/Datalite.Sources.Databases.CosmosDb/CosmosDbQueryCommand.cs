using System.Linq;
using System.Threading.Tasks;
using Datalite.Destination;

namespace Datalite.Sources.Databases.CosmosDb
{
    /// <summary>
    /// Configures the context for a query.
    /// </summary>
    public class CosmosDbQueryCommand
    {
        private readonly CosmosDbDataliteContext _context;

        internal CosmosDbQueryCommand(
            CosmosDbDataliteContext context)
        {
            _context = context;
        }

        /// <summary>
        /// Use the provided <paramref name="columns"/> in the output table.
        /// </summary>
        /// <param name="columns">The columns that the output table will contain.</param>
        /// <returns></returns>
        public CosmosDbQueryCommand WithColumns(params Column[] columns)
        {
            _context.TableDefinition = new TableDefinition(_context.OutputTable);

            foreach (var column in columns)
            {
                _context.TableDefinition.Columns[column.Name] = column;
            }

            return this;
        }

        /// <summary>
        /// Don't serialize nested objects or arrays as strings. Such fields will
        /// not be included in the output table as columns.
        /// </summary>
        /// <returns></returns>
        public CosmosDbQueryCommand IgnoreNested()
        {
            _context.SerializeNested = false;
            return this;
        }

        /// <summary>
        /// Add an individual index that covers all the specified column names.
        /// </summary>
        /// <param name="columns">The columns to be included in this individual index.</param>
        /// <returns></returns>
        public CosmosDbQueryCommand AddIndex(params string[] columns)
        {
            if (!_context.Indexes.Any(x => x.SequenceEqual(columns)))
            {
                _context.Indexes.Add(columns);
            }
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