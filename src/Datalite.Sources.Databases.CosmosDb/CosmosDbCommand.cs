namespace Datalite.Sources.Databases.CosmosDb
{
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
            return new CosmosDbQueryCommand(_context);
        }
    }
}