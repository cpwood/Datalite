namespace Datalite.Sources.Databases.AzureTables
{
    public class AzureTablesCommand
    {
        private readonly AzureTablesDataliteContext _context;

        internal AzureTablesCommand(AzureTablesDataliteContext context)
        {
            _context = context;
        }

        public AzureMultipleTablesCommand FromTables()
        {
            _context.Mode = AzureTablesDataliteContext.CommandType.Tables;
            return new AzureMultipleTablesCommand(_context);
        }

        public AzureMultipleTablesCommand FromTables(params string[] tables)
        {
            _context.Mode = AzureTablesDataliteContext.CommandType.Tables;
            return new AzureMultipleTablesCommand(_context);
        }

        public AzureSingleTableCommand FromTable(string tableName)
        {
            _context.Mode = AzureTablesDataliteContext.CommandType.Table;
            return new AzureSingleTableCommand(_context);
        }
    }
}