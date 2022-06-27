using System.Linq;

namespace Datalite.Sources.Databases.Shared
{
    public class DatabaseCommand
    {
        private readonly DatabaseDataliteContext _context;

        public DatabaseCommand(DatabaseDataliteContext context)
        {
            _context = context;
        }

        /// <summary>
        /// Copy all the user tables in the database to Sqlite.
        /// </summary>
        /// <returns></returns>
        public DatabaseTablesCommand FromTables()
        {
            _context.Mode = DatabaseDataliteContext.CommandType.Tables;
            return new DatabaseTablesCommand(_context);
        }

        /// <summary>
        /// Copy the tables with the provided names to Sqlite. Source table names are generally
        /// case sensitive, but this does depend on the souce database platform.
        /// </summary>
        /// <param name="tables">An array of table names.</param>
        /// <returns></returns>
        public DatabaseTablesCommand FromTables(params string[] tables)
        {
            _context.Mode = DatabaseDataliteContext.CommandType.Tables;
            _context.Tables = tables.Select(x => new TableIdentifier(x)).ToArray();
            return new DatabaseTablesCommand(_context);
        }

        /// <summary>
        /// Copy the tables with the provided identifiers to Sqlite. Source table names are generally
        /// case sensitive, but this does depend on the souce database platform.
        /// </summary>
        /// <param name="tables">An array of table identifiers.</param>
        /// <returns></returns>
        public DatabaseTablesCommand FromTables(params TableIdentifier[] tables)
        {
            _context.Mode = DatabaseDataliteContext.CommandType.Tables;
            _context.Tables = tables;
            return new DatabaseTablesCommand(_context);
        }

        /// <summary>
        /// Copy the table with the provided name to Sqlite. The source table name is generally
        /// case sensitive, but this does depend on the souce database platform.
        /// </summary>
        /// <param name="tableName"></param>
        /// <returns></returns>
        public DatabaseTableCommand FromTable(string tableName)
        {
            _context.Mode = DatabaseDataliteContext.CommandType.Table;
            _context.Table = new TableIdentifier(tableName);
            return new DatabaseTableCommand(_context);
        }

        /// <summary>
        /// Copy the table with the provided identifier to Sqlite. Source schema and table names are generally
        /// case sensitive, but this does depend on the souce database platform.
        /// </summary>
        /// <param name="table"></param>
        /// <returns></returns>
        public DatabaseTableCommand FromTable(TableIdentifier table)
        {
            _context.Mode = DatabaseDataliteContext.CommandType.Table;
            _context.Table = table;
            return new DatabaseTableCommand(_context);
        }

        /// <summary>
        /// Run the provided SQL and store the results in a Sqlite output table.  Source table names are generally
        /// case sensitive, but this does depend on the souce database platform.
        /// </summary>
        /// <param name="sql">The SQL to execute.</param>
        /// <param name="outputTable">The name of the Sqlite output table.</param>
        /// <returns></returns>
        public DatabaseQueryCommand FromQuery(string sql, string outputTable)
        {
            _context.Mode = DatabaseDataliteContext.CommandType.Query;
            _context.Sql = sql;
            _context.OutputTable = outputTable;
            return new DatabaseQueryCommand(_context);
        }
    }
}