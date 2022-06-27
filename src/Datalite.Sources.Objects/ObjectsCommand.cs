using System.Threading.Tasks;
using Datalite.Destination;

namespace Datalite.Sources.Objects
{
    public class ObjectsCommand
    {
        private readonly ObjectsDataliteContext _context;

        internal ObjectsCommand(ObjectsDataliteContext context)
        {
            _context = context;
        }

        public ObjectsCommand WithColumns(params Column[] columns)
        {
            _context.TableDefinition = new TableDefinition(_context.TableName);

            foreach (var column in columns)
            {
                _context.TableDefinition.Columns[column.Name] = column;
            }

            return this;
        }

        public ObjectsCommand AddIndex(params string[] columns)
        {
            _context.Indexes.Add(columns);
            return this;
        }

        public ObjectsCommand IgnoreNested()
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