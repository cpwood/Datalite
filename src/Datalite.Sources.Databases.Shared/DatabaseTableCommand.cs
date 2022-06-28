using System.Threading.Tasks;

namespace Datalite.Sources.Databases.Shared
{
    /// <summary>
    /// Configures the context for a single table.
    /// </summary>
    public class DatabaseTableCommand
    {
        private readonly DatabaseDataliteContext _context;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="context"></param>
        internal DatabaseTableCommand(
            DatabaseDataliteContext context)
        {
            _context = context;
        }

        /// <summary>
        /// Automatically find columns that are part of a primary key, foreign key or index so that
        /// the output table can receive equivalent indexes.
        /// </summary>
        /// <returns></returns>
        public DatabaseTableCommand WithAutomaticIndexes()
        {
            _context.AutoIndexes = true;
            return this;
        }

        /// <summary>
        /// Add an individual index that covers all the specified column names.
        /// </summary>
        /// <param name="columns">The columns to be included in this individual index.</param>
        /// <returns></returns>
        public DatabaseTableCommand AddIndex(params string[] columns)
        {
            _context.Indexes.Add(columns);
            return this;
        }

        /// <summary>
        /// Configures the output table name. If this isn't configured, the output table
        /// name will be the same as the input table name.
        /// </summary>
        /// <param name="tableName"></param>
        /// <returns></returns>
        public DatabaseTableCommand ToTable(string tableName)
        {
            _context.OutputTable = tableName;
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