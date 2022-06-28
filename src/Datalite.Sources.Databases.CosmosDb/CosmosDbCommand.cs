namespace Datalite.Sources.Databases.CosmosDb
{
    /// <summary>
    /// Configures the context for Azure CosmosDB.
    /// </summary>
    public class CosmosDbCommand
    {
        private readonly CosmosDbDataliteContext _context;

        internal CosmosDbCommand(CosmosDbDataliteContext context)
        {
            _context = context;
        }

        /// <summary>
        /// Load data using a SQL query into a Sqlite table.
        /// </summary>
        /// <param name="sql">The query to perform.</param>
        /// <param name="outputTable">The Sqlite destination table name.</param>
        /// <returns></returns>
        public CosmosDbQueryCommand FromQuery(string sql, string outputTable)
        {
            _context.Sql = sql;
            _context.OutputTable = outputTable;
            return new CosmosDbQueryCommand(_context);
        }
    }
}