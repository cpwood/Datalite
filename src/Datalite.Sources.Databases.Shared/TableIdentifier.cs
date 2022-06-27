namespace Datalite.Sources.Databases.Shared
{
    public class TableIdentifier
    {
        private TableIdentifier()
        {
            TableName = string.Empty;
        }

        public TableIdentifier(string schemaName, string tableName)
        {
            SchemaName = schemaName;
            TableName = tableName;
        }

        public TableIdentifier(string tableName)
        {
            TableName = tableName;
        }

        public string? SchemaName { get; set; }
        public string TableName { get; set; }
    }
}
