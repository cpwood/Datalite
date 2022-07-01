using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Dapper;
using Datalite.Destination;
using Datalite.Exceptions;
using Datalite.Sources.Databases.Shared;
using Npgsql;

namespace Datalite.Sources.Databases.Postgres
{
    internal class PostgresService : DatabaseService
    {
        private readonly NpgsqlConnection _sourceConnection;

        public PostgresService(SqliteConnectionBroker sqliteConnection, NpgsqlConnection sourceConnection) 
            : base(sqliteConnection, sourceConnection)
        {
            _sourceConnection = sourceConnection;
        }

        public override bool CanGetAllTables => true;
        public override bool CanDiscoverTableIndexes => true;

        public override async Task<TableIdentifier[]> GetAllTablesAsync()
        {
            return (await _sourceConnection.QueryAsync<TableIdentifier>(@"
                SELECT      TABLE_SCHEMA AS SchemaName,
                            TABLE_NAME AS TableName
                FROM        INFORMATION_SCHEMA.TABLES
                WHERE       TABLE_TYPE = 'BASE TABLE'
                AND         TABLE_SCHEMA NOT IN ('pg_catalog', 'information_schema')
                ORDER BY    TABLE_NAME")).ToArray();
        }

        public override Task<TableDefinition> GetDefinitionFromSql(string sql, string outputTable)
        {
            // Don't return any data - we just want the schema back.
            // Wrap the query and add a WHERE clause that can never
            // be true to achieve this. This should return in milliseconds
            // regardless of the volume of data the original SQL would
            // return.
            sql = $"SELECT * FROM ({sql}) AS XXYYZZ WHERE 1 = 2";

            using var da = new NpgsqlDataAdapter(sql, _sourceConnection);
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

        public override Task<TableDefinition> GetDefinitionFromTable(TableIdentifier inputTable, string outputTable)
        {
            var sql = $"SELECT * FROM {ToTableName(inputTable)}";
            return GetDefinitionFromSql(sql, outputTable);
        }

        public override Task<IDataReader> OpenReaderForTableAsync(TableIdentifier tableIdentifier)
        {
            return OpenReaderForQueryAsync($"SELECT * FROM {ToTableName(tableIdentifier)}");
        }

        public override async Task<IDataReader> OpenReaderForQueryAsync(string sql)
        {
            var cmd = new NpgsqlCommand(sql, _sourceConnection);
            return await cmd.ExecuteReaderAsync();
        }

        public override async Task<IEnumerable<string[]>> DiscoverTableIndexesAsync(TableIdentifier tableIdentifier)
        {
            var sql = @$"
                    SELECT      i.relname AS Name,
                                case when ix.indisprimary = true then 'PrimaryKey' else 'Index' end as OriginalType,
                                a.attname AS ColumnName,
                                (
                                    SELECT i
                                    FROM
                                        (
                                        SELECT      *,
                                                    row_number() OVER () i
                                        FROM        unnest(indkey)
                                        WITH        ORDINALITY AS a(v)
                                        ) a
                                    WHERE v = attnum
                                ) as ColumnOrder
                    FROM        pg_class t
                    LEFT JOIN   pg_index ix ON t.oid = ix.indrelid
                    LEFT JOIN   pg_class i ON i.oid = ix.indexrelid
                    LEFT JOIN   pg_attribute a ON a.attrelid = t.oid
                    LEFT JOIN   pg_catalog.pg_namespace n ON n.oid = t.relnamespace
                    WHERE       a.attnum = ANY (ix.indkey)
                    AND         t.relkind = 'r'
                    AND         n.nspname = '{tableIdentifier.SchemaName ?? "public"}'
                    AND         t.relname = '{tableIdentifier.TableName}'
                    ORDER BY    Name,
                                ColumnOrder";

            var candidates = await _sourceConnection.QueryAsync<IndexCandidate>(sql);
            return BuildIndexesFromCandidates(candidates);
        }

        public override string ToTableName(TableIdentifier tableIdentifier)
        {
            return string.IsNullOrEmpty(tableIdentifier.SchemaName)
                ? $"\"{tableIdentifier.TableName}\""
                : $"\"{tableIdentifier.SchemaName}\".\"{tableIdentifier.TableName}\"";
        }

        public override string ToSqliteTableName(TableIdentifier tableIdentifier)
        {
            if (string.IsNullOrEmpty(tableIdentifier.SchemaName) || tableIdentifier.SchemaName == "public")
                return tableIdentifier.TableName;

            return $"{tableIdentifier.SchemaName}_{tableIdentifier.TableName}";
        }

        public override void ValidateTableIdentifier(TableIdentifier tableIdentifier)
        {
            var regex = new Regex(@"^[\p{L}_]\S{0,30}$");

            if (!regex.IsMatch(tableIdentifier.TableName))
                throw new DataliteException($"The provided table name, '{tableIdentifier.TableName}', is invalid.");

            if (!string.IsNullOrEmpty(tableIdentifier.SchemaName) && !regex.IsMatch(tableIdentifier.SchemaName))
                throw new DataliteException($"The provided schema name, '{tableIdentifier.SchemaName}', is invalid.");
        }
    }
}
