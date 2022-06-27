using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Odbc;
using System.Linq;
using System.Threading.Tasks;
using Dapper;
using Datalite.Destination;
using Datalite.Sources.Databases.Shared;

namespace Datalite.Sources.Databases.Odbc
{
    /// <summary>
    /// <para>
    /// Performs the majority of the work for dealing with ODBC data sources. This class
    /// can be inherited from to tailor the experience for specific data sources.
    /// </para>
    /// <para>
    /// For example, this generic ODBC service cannot discover all the tables in a database or obtain the
    /// list of indexes and keys for a table since that is specific to a database implementation. The
    /// class that inherits from <see cref="OdbcService"/> can provide the implementation details to
    /// make this possible.
    /// </para>
    /// </summary>
    public class OdbcService : DatabaseService
    {
        private readonly OdbcConnection _sourceConnection;

        /// <summary>
        /// Creates a new instance of this service.
        /// </summary>
        /// <param name="sqliteConnection">The Sqlite connection.</param>
        /// <param name="sourceConnection">The <see cref="OdbcConnection"/>.</param>
        public OdbcService(
            SqliteConnectionBroker sqliteConnection, 
            OdbcConnection sourceConnection)
            : base(sqliteConnection, sourceConnection)
        {
            _sourceConnection = sourceConnection;
            CanDiscoverTableIndexes = false;
            CanGetAllTables = false;
        }

        /// <summary>
        /// Creates a new instance of this service.
        /// </summary>
        /// <param name="sqliteConnection">The Sqlite connection.</param>
        /// <param name="sourceConnection">The <see cref="OdbcConnection"/>.</param>
        /// <param name="canDiscoverTableIndexes">Configures whether it is possible to discover table indexes from this ODBC data source. Relies upon this class being inherited from and <see cref="IndexesSql"/> being overridden.</param>
        /// <param name="canGetAllTables">Configures whether it is possible to discover a list of tables from this ODBC data source. Relies upon this class being inherited from and <see cref="AllTablesSql"/> being overridden.</param>
        public OdbcService(
            SqliteConnectionBroker sqliteConnection,
            OdbcConnection sourceConnection,
            bool canDiscoverTableIndexes,
            bool canGetAllTables)
            : base(sqliteConnection, sourceConnection)
        {
            _sourceConnection = sourceConnection;
            CanDiscoverTableIndexes = canDiscoverTableIndexes;
            CanGetAllTables = canGetAllTables;
        }

        /// <inheritdoc />
        public override bool CanGetAllTables { get; }

        /// <inheritdoc />
        public override bool CanDiscoverTableIndexes { get; }

        /// <summary>
        /// The SQL that will provide a list of table names in a single column of results.
        /// </summary>
        public virtual string AllTablesSql => string.Empty;

        /// <summary>
        /// The SQL that will provide a list of index candidates. The output columns must map to properties on an <see cref="IndexCandidate"/> object.
        /// </summary>
        public virtual string IndexesSql => string.Empty;
        
        /// <summary>
        /// The SQL to be used to determine the columns in a table and their types. Must contain {table} as a placeholder for the table name.
        /// </summary>
        public virtual string TableDefinitionSql => "SELECT * FROM {table} WHERE 1 = 2";

        /// <summary>
        /// The SQL to be used to determine the columns in a query resultset and their types. Must contain {sql} as a placeholder for the query.
        /// </summary>
        public virtual string QueryTableDefinitionSql => "SELECT * FROM ({sql}) AS XXYYZZ WHERE 1 = 2";

        /// <summary>
        /// The default schema name for the database - e.g. dbo.
        /// </summary>
        public virtual string DefaultSchemaName => "dbo";

        /// <inheritdoc />
        public override async Task<TableIdentifier[]> GetAllTablesAsync()
        {
            if (!CanGetAllTables) 
                throw new NotSupportedException();

            return (await _sourceConnection.QueryAsync<TableIdentifier>(AllTablesSql)).ToArray();
        }

        /// <inheritdoc />
        public override Task<TableDefinition> GetDefinitionFromSql(string sql, string outputTable)
        {
            sql = QueryTableDefinitionSql.Replace("{sql}", sql);
            return GetDefinitionInternal(sql, outputTable);
        }

        /// <inheritdoc />
        public override Task<TableDefinition> GetDefinitionFromTable(TableIdentifier inputTable, string outputTable)
        {
            var sql = TableDefinitionSql.Replace("{table}", ToTableName(inputTable));
            return GetDefinitionInternal(sql, outputTable);
        }

        private Task<TableDefinition> GetDefinitionInternal(string sql, string outputTable)
        {
            using var da = new OdbcDataAdapter(sql, _sourceConnection);
            var dt = new DataTable();
            da.Fill(dt);

            var definition = new TableDefinition(outputTable);

            foreach (DataColumn column in dt.Columns)
            {
                definition.Columns[column.ColumnName] = new Column(column.ColumnName, column.DataType,
                    !column.AllowDBNull);
            }

            return Task.FromResult(definition);
        }

        /// <inheritdoc />
        public override Task<IDataReader> OpenReaderForTableAsync(TableIdentifier tableIdentifier)
        {
            return OpenReaderForQueryAsync($"SELECT * FROM {ToTableName(tableIdentifier)}");
        }

        /// <inheritdoc />
        public override Task<IDataReader> OpenReaderForQueryAsync(string sql)
        {
            var cmd = new OdbcCommand(sql, _sourceConnection);
            return Task.FromResult((IDataReader)cmd.ExecuteReader());
        }

        /// <inheritdoc />
        public override async Task<IEnumerable<string[]>> DiscoverTableIndexesAsync(TableIdentifier tableIdentifier)
        {
            if (!CanDiscoverTableIndexes)
                throw new NotSupportedException();
            
            var sql = IndexesSql.Replace("{table}", tableIdentifier.TableName);

            if (!string.IsNullOrEmpty(tableIdentifier.SchemaName))
                sql = sql.Replace("{schema}", tableIdentifier.SchemaName);

            var candidates = await _sourceConnection.QueryAsync<IndexCandidate>(sql);
            return BuildIndexesFromCandidates(candidates);
        }

        public override string ToTableName(TableIdentifier tableIdentifier)
        {
            // We don't know the quote character for identifiers, so don't attempt to use one.
            return string.IsNullOrEmpty(tableIdentifier.SchemaName) || tableIdentifier.SchemaName == DefaultSchemaName
                ? tableIdentifier.TableName
                : $"{tableIdentifier.SchemaName}.{tableIdentifier.TableName}";
        }

        public override string ToSqliteTableName(TableIdentifier tableIdentifier)
        {
            if (string.IsNullOrEmpty(tableIdentifier.SchemaName) || tableIdentifier.SchemaName == DefaultSchemaName)
                return tableIdentifier.TableName;

            return $"{tableIdentifier.SchemaName}_{tableIdentifier.TableName}";
        }

        public override void ValidateTableIdentifier(TableIdentifier tableIdentifier)
        {
            // Don't know enough about the target database system to judge this.
        }
    }
}
