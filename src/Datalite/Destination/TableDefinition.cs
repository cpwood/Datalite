using System.Collections.Generic;
using System.Linq;

namespace Datalite.Destination
{
    /// <summary>
    /// Defines the Sqlite table and the columns that will be used
    /// to store the data obtained from the data source.
    /// </summary>
    public class TableDefinition
    {
        /// <summary>
        /// The name of the table.
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// The columns in the table.
        /// </summary>
        public Dictionary<string, Column> Columns { get; set; }

        /// <summary>
        /// Create a new <see cref="TableDefinition"/> for the table with the given <paramref name="name"/>.
        /// </summary>
        /// <param name="name"></param>
        public TableDefinition(string name)
        {
            Name = name;
            Columns = new Dictionary<string, Column>();
        }

        /// <summary>
        /// Returns the DDL that can be used to create the Sqlite table.
        /// </summary>
        /// <returns>The DDL that can be used to create the Sqlite table.</returns>
        public override string ToString()
        {
            var cols = Columns.Values.Select(x => $"\"{x.Name}\" {x.StorageClass.AsString()} {(x.Required ? "NOT NULL" : "NULL")}");
            return $"CREATE TABLE IF NOT EXISTS \"{Name}\" ({string.Join(',', cols)});";
        }
    }
}