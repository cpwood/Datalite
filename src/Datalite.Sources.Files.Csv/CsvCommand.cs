using System;
using System.Threading.Tasks;
using CsvHelper.Configuration;
using Datalite.Destination;

namespace Datalite.Sources.Files.Csv
{
    public class CsvCommand
    {
        private readonly CsvDataliteContext _context;

        internal CsvCommand(CsvDataliteContext context)
        {
            _context = context;
        }

        public CsvCommand WithColumns(params Column[] columns)
        {
            _context.TableDefinition = new TableDefinition(_context.OutputTable);

            foreach (var column in columns)
            {
                _context.TableDefinition.Columns[column.Name] = column;
            }

            return this;
        }

        public CsvCommand AddIndex(params string[] columns)
        {
            _context.Indexes.Add(columns);
            return this;
        }

        public CsvCommand WithOptions(Action<CsvConfiguration> configuration)
        {
            _context.Configuration = configuration;
            return this;
        }

        public Task ExecuteAsync()
        {
            return _context.ExecuteAsync();
        }
    }
}