using System;

namespace Datalite.Destination
{
    /// <summary>
    /// Defines a column in a Sqlite table.
    /// </summary>
    public class Column
    {
        /// <summary>
        /// The name of the column.
        /// </summary>
        public string Name { get; }

        /// <summary>
        /// The Sqlite storage class used by the column.
        /// </summary>
        public StoragesClasses.StorageClassType StorageClass { get; }

        /// <summary>
        /// Whether this column must have a value.
        /// </summary>
        public bool Required { get; }

        /// <summary>
        /// The .NET CLR <see cref="Type"/> that will be used for this column.
        /// </summary>
        public Type Type { get; }

        /// <summary>
        /// Creates a new <see cref="Column"/> instance.
        /// </summary>
        /// <param name="name">The name of the column.</param>
        /// <param name="type">The .NET CLR <see cref="Type"/> that will be used for this column.</param>
        /// <param name="required">Whether this column must have a value.</param>
        public Column(string name, Type type, bool required)
        {
            Name = name;
            Type = type;
            StorageClass = StoragesClasses.FromType(type);
            Required = required;
        }

        /// <summary>
        /// Creates a new <see cref="Column"/> instance.
        /// </summary>
        /// <param name="name">The name of the column.</param>
        /// <param name="type">The .NET CLR <see cref="Type"/> that will be used for this column.</param>
        /// <param name="storageType">The Sqlite storage class used by the column.</param>
        /// <param name="required">Whether this column must have a value.</param>
        public Column(string name, Type type, StoragesClasses.StorageClassType storageType, bool required)
        {
            Name = name;
            Type = type;
            StorageClass = storageType;
            Required = required;
        }
    }
}