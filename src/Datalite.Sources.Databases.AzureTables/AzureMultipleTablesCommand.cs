using System.Threading.Tasks;

namespace Datalite.Sources.Databases.AzureTables
{
    /// <summary>
    /// Configures the context for multiple tables.
    /// </summary>
    public class AzureMultipleTablesCommand
    {
        private readonly AzureTablesDataliteContext _context;

        internal AzureMultipleTablesCommand(AzureTablesDataliteContext context)
        {
            _context = context;
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