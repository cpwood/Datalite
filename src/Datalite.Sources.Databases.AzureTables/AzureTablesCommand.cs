namespace Datalite.Sources.Databases.AzureTables
{
    /// <summary>
    /// Configures the context for Azure Table Storage.
    /// </summary>
    public class AzureTablesCommand
    {
        private readonly AzureTablesDataliteContext _context;

        internal AzureTablesCommand(AzureTablesDataliteContext context)
        {
            _context = context;
        }

        /// <summary>
        /// Copy all the tables in the storage account to Sqlite.
        /// </summary>
        /// <returns></returns>
        public AzureMultipleTablesCommand FromTables()
        {
            _context.Mode = AzureTablesDataliteContext.CommandType.Tables;
            return new AzureMultipleTablesCommand(_context);
        }

        /// <summary>
        /// Copy the tables with the provided names to Sqlite.
        /// </summary>
        /// <param name="tables">An array of table names.</param>
        /// <returns></returns>
        public AzureMultipleTablesCommand FromTables(params string[] tables)
        {
            _context.Mode = AzureTablesDataliteContext.CommandType.Tables;
            _context.Tables = tables;
            return new AzureMultipleTablesCommand(_context);
        }

        /// <summary>
        /// Copy the table with the provided name to Sqlite. 
        /// </summary>
        /// <param name="tableName"></param>
        /// <returns></returns>
        public AzureSingleTableCommand FromTable(string tableName)
        {
            _context.Mode = AzureTablesDataliteContext.CommandType.Table;
            _context.Table = tableName;
            return new AzureSingleTableCommand(_context);
        }
    }
}