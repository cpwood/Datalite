using System;
using System.Threading.Tasks;

namespace Datalite.Sources.Databases.Shared
{
    /// <summary>
    /// Contains the configuration values for a data migration.
    /// </summary>
    public class DatabaseDataliteContext : DataliteContext
    {
        /// <summary>
        /// The data source for a migration.
        /// </summary>
        public enum CommandType
        {
            /// <summary>
            /// Multiple tables
            /// </summary>
            Tables,

            /// <summary>
            /// A single table
            /// </summary>
            Table,

            /// <summary>
            /// An SQL query
            /// </summary>
            Query
        }

        /// <summary>
        /// The data source for this migration.
        /// </summary>
        public CommandType Mode { get; set; }

        /// <summary>
        /// Whether Datalite created the <see cref="System.Data.Common.DbConnection"/> object
        /// for this migration and hence whether it should also destroy it when the work is
        /// complete.
        /// </summary>
        public bool CreatedConnection { get; }

        /// <summary>
        /// The source table name.
        /// </summary>
        public TableIdentifier? Table { get; set; }

        /// <summary>
        /// The names of the source tables.
        /// </summary>
        public TableIdentifier[] Tables { get; set; } = Array.Empty<TableIdentifier>();

        /// <summary>
        /// The Sqlite destination table name.
        /// </summary>
        public string OutputTable { get; set; } = string.Empty;

        /// <summary>
        /// The SQL query to be used to create the data source for the migration.
        /// </summary>
        public string Sql { get; set; } = string.Empty;

        /// <summary>
        /// Whether Datalite should attempt to discover indexes for the source table(s).
        /// </summary>
        public bool AutoIndexes { get; set; }

        /// <summary>
        /// Constructor for the context.
        /// </summary>
        /// <param name="createdConnection">Whether Datalite created the database connection.</param>
        /// <param name="executionFunction">The function to execute upon completion of the configuration.</param>
        public DatabaseDataliteContext(
            bool createdConnection,
            Func<DatabaseDataliteContext, Task> executionFunction)
            : base(c => executionFunction((DatabaseDataliteContext)c))
        {
            CreatedConnection = createdConnection;
        }
    }
}
