using System;
using System.Threading.Tasks;
using CsvHelper.Configuration;
using Datalite.Destination;

namespace Datalite.Sources.Files.Csv
{
    /// <summary>
    /// Configures the context for a CSV file.
    /// </summary>
    public class CsvCommand
    {
        private readonly CsvDataliteContext _context;

        internal CsvCommand(CsvDataliteContext context)
        {
            _context = context;
        }

        /// <summary>
        /// Use the provided column definitions to create the Sqlite output table.
        /// </summary>
        /// <param name="columns">The column settings.</param>
        /// <returns></returns>
        public CsvCommand WithColumns(params Column[] columns)
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
        public CsvCommand AddIndex(params string[] columns)
        {
            _context.Indexes.Add(columns);
            return this;
        }

        /// <summary>
        /// Configure the CSV reading options. Uses a CsvHelper <see cref="CsvConfiguration"/> object.
        /// See <see href="https://github.com/JoshClose/CsvHelper/blob/master/src/CsvHelper/Configuration/IReaderConfiguration.cs"/> for detail.
        /// </summary>
        /// <param name="configuration"></param>
        /// <returns></returns>
        public CsvCommand WithOptions(Action<CsvConfiguration> configuration)
        {
            _context.Configuration = configuration;
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