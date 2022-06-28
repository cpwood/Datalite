namespace Datalite.Sources.Databases.Shared
{
    /// <summary>
    /// Describes a table in the source database.
    /// </summary>
    public class TableIdentifier
    {
        // ReSharper disable once UnusedMember.Local
        private TableIdentifier()
        {
            TableName = string.Empty;
        }

        /// <summary>
        /// Creates a record with the provided schema and table names.
        /// </summary>
        /// <param name="schemaName">The schema name.</param>
        /// <param name="tableName">The table name.</param>
        public TableIdentifier(string schemaName, string tableName)
        {
            SchemaName = schemaName;
            TableName = tableName;
        }

        /// <summary>
        /// Creates a record with the provided table name.
        /// </summary>
        /// <param name="tableName">The table name.</param>
        public TableIdentifier(string tableName)
        {
            TableName = tableName;
        }

        /// <summary>
        /// The schema name.
        /// </summary>
        public string? SchemaName { get; set; }

        /// <summary>
        /// The table name.
        /// </summary>
        public string TableName { get; set; }
    }
}
