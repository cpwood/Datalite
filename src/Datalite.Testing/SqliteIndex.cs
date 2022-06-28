namespace Datalite.Testing
{
    /// <summary>
    /// An index on a Sqlite table.
    /// </summary>
    public class SqliteIndex
    {
        /// <summary>
        /// The name of the index.
        /// </summary>
        public string IndexName { get; }

        /// <summary>
        /// The name of the column.
        /// </summary>
        public string ColumnName { get; }

        /// <summary>
        /// The order of the column within the index.
        /// </summary>
        public int ColumnOrder { get; }

        /// <summary>
        /// Creates an index record.
        /// </summary>
        /// <param name="indexName">The name of the index.</param>
        /// <param name="columnName">The name of the column.</param>
        /// <param name="columnOrder">The order of the column within the index.</param>
        public SqliteIndex(string indexName, string columnName, int columnOrder)
        {
            IndexName = indexName;
            ColumnName = columnName;
            ColumnOrder = columnOrder;
        }
    }
}
