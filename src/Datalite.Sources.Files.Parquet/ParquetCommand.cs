using System.Threading.Tasks;

namespace Datalite.Sources.Files.Parquet
{
    public class ParquetCommand
    {
        private readonly ParquetDataliteContext _context;

        internal ParquetCommand(ParquetDataliteContext context)
        {
            _context = context;
        }

        public ParquetCommand AddIndex(params string[] columns)
        {
            _context.Indexes.Add(columns);
            return this;
        }

        public Task ExecuteAsync()
        {
            return _context.ExecuteAsync();
        }
    }
}