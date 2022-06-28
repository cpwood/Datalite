using System.Threading.Tasks;
using Datalite.Destination;

namespace Datalite.Sources.Files.Json
{
    /// <summary>
    /// Configures the context for a JSON file.
    /// </summary>
    public class JsonCommand
    {
        private readonly JsonDataliteContext _context;

        internal JsonCommand(JsonDataliteContext context)
        {
            _context = context;
        }

        /// <summary>
        /// Use the provided column definitions to create the Sqlite output table.
        /// </summary>
        /// <param name="columns">The column settings.</param>
        /// <returns></returns>
        public JsonCommand WithColumns(params Column[] columns)
        {
            _context.TableDefinition = new TableDefinition(_context.OutputTable);

            foreach (var column in columns)
            {
                _context.TableDefinition.Columns[column.Name] = column;
            }

            return this;
        }

        /// <summary>
        /// Add an individual index that covers all the specified column names.
        /// </summary>
        /// <param name="columns">The columns to be included in this individual index.</param>
        /// <returns></returns>
        public JsonCommand AddIndex(params string[] columns)
        {
            _context.Indexes.Add(columns);
            return this;
        }

        /// <summary>
        /// Don't serialize object child object and array values into string columns; disregard
        /// them altogether.
        /// </summary>
        /// <returns></returns>
        public JsonCommand IgnoreNested()
        {
            _context.SerializeNested = false;
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