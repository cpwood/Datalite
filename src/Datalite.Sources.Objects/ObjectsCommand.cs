using System.Threading.Tasks;
using Datalite.Destination;

namespace Datalite.Sources.Objects
{
    /// <summary>
    /// Configures the context for .NET CLR objects.
    /// </summary>
    public class ObjectsCommand
    {
        private readonly ObjectsDataliteContext _context;

        internal ObjectsCommand(ObjectsDataliteContext context)
        {
            _context = context;
        }

        /// <summary>
        /// Use the provided column definitions to create the Sqlite output table.
        /// </summary>
        /// <param name="columns">The column settings.</param>
        /// <returns></returns>
        public ObjectsCommand WithColumns(params Column[] columns)
        {
            _context.TableDefinition = new TableDefinition(_context.TableName);

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
        public ObjectsCommand AddIndex(params string[] columns)
        {
            _context.Indexes.Add(columns);
            return this;
        }

        /// <summary>
        /// Don't serialize object child object and array values into string columns; disregard
        /// them altogether.
        /// </summary>
        /// <returns></returns>
        public ObjectsCommand IgnoreNested()
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