using System.Threading.Tasks;
using Datalite.Destination;

namespace Datalite.Sources.Files.Json
{
    public class JsonCommand
    {
        private readonly JsonDataliteContext _context;

        internal JsonCommand(JsonDataliteContext context)
        {
            _context = context;
        }

        public JsonCommand WithColumns(params Column[] columns)
        {
            _context.TableDefinition = new TableDefinition(_context.OutputTable);

            foreach (var column in columns)
            {
                _context.TableDefinition.Columns[column.Name] = column;
            }

            return this;
        }

        public JsonCommand AddIndex(params string[] columns)
        {
            _context.Indexes.Add(columns);
            return this;
        }

        public JsonCommand IgnoreNested()
        {
            _context.SerializeNested = false;
            return this;
        }

        public Task ExecuteAsync()
        {
            return _context.ExecuteAsync();
        }
    }
}