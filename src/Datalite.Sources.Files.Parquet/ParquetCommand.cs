using System.Threading.Tasks;

namespace Datalite.Sources.Files.Parquet
{
    /// <summary>
    /// Configures the context for a Parquet file.
    /// </summary>
    public class ParquetCommand
    {
        private readonly ParquetDataliteContext _context;

        internal ParquetCommand(ParquetDataliteContext context)
        {
            _context = context;
        }

        /// <summary>
        /// Add an individual index that covers all the specified column names.
        /// </summary>
        /// <param name="columns">The columns to be included in this individual index.</param>
        /// <returns></returns>
        public ParquetCommand AddIndex(params string[] columns)
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