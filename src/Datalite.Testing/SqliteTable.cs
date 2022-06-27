using System;
using System.Collections.Generic;

namespace Datalite.Testing
{
    /// <summary>
    /// Defines the Sqlite table and the columns that will be used
    /// to store the data obtained from the data source.
    /// </summary>
    public class SqliteTable
    {
        /// <summary>
        /// Create an object representing a Sqlite table.
        /// </summary>
        /// <param name="name">The table name.</param>
        public SqliteTable(string name)
        {
            Name = name;
        }

        /// <summary>
        /// The name of the table.
        /// </summary>
        public string Name { get; }

        /// <summary>
        /// The columns in the table.
        /// </summary>
        public Dictionary<string, SqliteColumn> Columns { get; set; } = new Dictionary<string, SqliteColumn>();

        /// <summary>
        /// The table indexes.
        /// </summary>
        public string[][] Indexes { get; set; } = Array.Empty<string[]>();

        /// <summary>
        /// The rows in the table.
        /// </summary>
        public string[][] Rows { get; set; } = Array.Empty<string[]>();
    }
}