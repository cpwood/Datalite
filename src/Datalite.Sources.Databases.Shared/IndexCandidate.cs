namespace Datalite.Sources.Databases.Shared
{
    /// <summary>
    /// Models a column that will be part of an index because it was originally
    /// part of a primary key, foreign key or index in the source database.
    /// </summary>
    public class IndexCandidate
    {
        /// <summary>
        /// The name of the database object.
        /// </summary>
        public string? Name { get; set; }

        /// <summary>
        /// Whether it is a PrimaryKey, ForeignKey or Index.
        /// </summary>
        public string? OriginalType { get; set; }

        /// <summary>
        /// The name of the column.
        /// </summary>
        public string? ColumnName { get; set; }

        /// <summary>
        /// The order of the column within the primary key, foreign key or index.
        /// </summary>
        public int? ColumnOrder { get; set; }
    }
}