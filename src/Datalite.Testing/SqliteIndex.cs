namespace Datalite.Testing
{
    public class SqliteIndex
    {
        public string IndexName { get; }
        public string ColumnName { get; }
        public int ColumnOrder { get; }

        public SqliteIndex(string indexName, string columnName, int columnOrder)
        {
            IndexName = indexName;
            ColumnName = columnName;
            ColumnOrder = columnOrder;
        }
    }
}
