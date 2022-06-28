using System.Threading.Tasks;

namespace Datalite.Sources.Databases.Shared
{
    /// <summary>
    /// Configures the context for multiple tables.
    /// </summary>
    public class DatabaseTablesCommand
    {
        private readonly DatabaseDataliteContext _context;

        internal DatabaseTablesCommand(
            DatabaseDataliteContext context)
        {
            _context = context;
        }

        /// <summary>
        /// Automatically find columns that are part of a primary key, foreign key or index so that
        /// the output table can receive equivalent indexes.
        /// </summary>
        /// <returns></returns>
        public DatabaseTablesCommand WithAutomaticIndexes()
        {
            _context.AutoIndexes = true;
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