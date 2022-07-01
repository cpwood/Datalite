using Datalite.Destination;

namespace Datalite.Testing
{
    /// <summary>
    /// A column in a Sqlite table.
    /// </summary>
    public class SqliteColumn
    {
        /// <summary>
        /// The name of the column.
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// The storage class of the column.
        /// </summary>
        public StoragesClasses.StorageClassType StorageClass { get; set; }

        /// <summary>
        /// Whether the column allows NULLs or if it is required.
        /// </summary>
        public bool Required { get; set; }

        /// <summary>
        /// Creates a column for a Sqlite table.
        /// </summary>
        /// <param name="name">The name of the column.</param>
        /// <param name="storageClass">The storage class of the column.</param>
        /// <param name="required">Whether the column allows NULLs or if it is required.</param>
        public SqliteColumn(string name, StoragesClasses.StorageClassType storageClass, bool required)
        {
            Name = name;
            StorageClass = storageClass;
            Required = required;
        }
    }
}
