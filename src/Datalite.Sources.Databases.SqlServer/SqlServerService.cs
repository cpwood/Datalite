using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Dapper;
using Datalite.Destination;
using Datalite.Exceptions;
using Datalite.Sources.Databases.Shared;

namespace Datalite.Sources.Databases.SqlServer
{
    internal class SqlServerService : DatabaseService
    {
        private readonly SqlConnection _sqlConnection;

        public SqlServerService(SqliteConnectionBroker sqliteConnection, SqlConnection sourceConnection) 
            : base(sqliteConnection, sourceConnection)
        {
            _sqlConnection = sourceConnection;
        }

        public override bool CanGetAllTables => true;
        public override bool CanDiscoverTableIndexes => true;

        public override async Task<TableIdentifier[]> GetAllTablesAsync()
        {
            return (await _sqlConnection.QueryAsync<TableIdentifier>(@"
                SELECT      TABLE_SCHEMA AS SchemaName,
                            TABLE_NAME AS TableName
                FROM        INFORMATION_SCHEMA.TABLES
                WHERE       TABLE_TYPE = 'BASE TABLE'
                ORDER BY    TABLE_NAME")).ToArray();
        }

        public override async Task<TableDefinition> GetDefinitionFromSql(string sql, string outputTable)
        {
            // Don't return any data - we just want the schema back.
            // Wrap the query and add a WHERE clause that can never
            // be true to achieve this. This should return in milliseconds
            // regardless of the volume of data the original SQL would
            // return.
            sql = $"SELECT * FROM ({sql}) AS XXYYZZ WHERE 1 = 2";

            var cmd = new SqlCommand(sql, _sqlConnection);
            cmd.CommandText = sql;

            await using var reader = await cmd.ExecuteReaderAsync();
            var schemaTable = reader.GetSchemaTable();

            if (schemaTable == null)
                throw new NullReferenceException($"Cannot get the schema for query \"{sql}\"");

            var definition = new TableDefinition(outputTable);

            foreach (DataRow row in schemaTable.Rows)
            {
                definition.Columns[(string)row[SchemaTableColumn.ColumnName]] = new Column(
                    (string)row[SchemaTableColumn.ColumnName],
                    (Type)row[SchemaTableColumn.DataType],
                    !(bool)row[SchemaTableColumn.AllowDBNull]);
            }

            return definition;
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
            var cmd = new SqlCommand(sql, _sqlConnection);
            var reader = await cmd.ExecuteReaderAsync();
            return reader;
        }

        public override async Task<IEnumerable<string[]>> DiscoverTableIndexesAsync(TableIdentifier tableIdentifier)
        {
            var sql = @$"
            SELECT
                 ind.name AS Name,
                 IIF(ind.is_primary_key = 1, 'PrimaryKey', 'Index') AS OriginalType,
                 col.name AS ColumnName,
                 ic.key_ordinal AS ColumnOrder
            FROM
                 sys.indexes ind
            INNER JOIN
                 sys.index_columns ic ON  ind.object_id = ic.object_id and ind.index_id = ic.index_id
            INNER JOIN
                 sys.columns col ON ic.object_id = col.object_id and ic.column_id = col.column_id
            INNER JOIN
                 sys.tables t ON ind.object_id = t.object_id
            INNER JOIN sys.schemas sch
                ON t.schema_id = sch.schema_id
            WHERE
                sch.name = '{tableIdentifier.SchemaName ?? "dbo"}'
            AND
                t.name = '{tableIdentifier.TableName}'
            UNION
            SELECT obj.name AS Name,
                   'ForeignKey' AS OriginalType,
                    col1.name AS ColumnName,
                   ROW_NUMBER() OVER (PARTITION BY obj.name ORDER BY col1.name) AS ColumnOrder
            FROM sys.foreign_key_columns fkc
            INNER JOIN sys.objects obj
                ON obj.object_id = fkc.constraint_object_id
            INNER JOIN sys.tables tab1
                ON tab1.object_id = fkc.parent_object_id
            INNER JOIN sys.schemas sch
                ON tab1.schema_id = sch.schema_id
            INNER JOIN sys.columns col1
                ON col1.column_id = parent_column_id AND col1.object_id = tab1.object_id
            WHERE
                sch.name = '{tableIdentifier.SchemaName ?? "dbo"}'
            AND
                tab1.name = '{tableIdentifier}'
            ORDER BY 1, 2, 4";

            var candidates = await _sqlConnection.QueryAsync<IndexCandidate>(sql);

            return BuildIndexesFromCandidates(candidates);
        }

        public override string ToTableName(TableIdentifier tableIdentifier)
        {
            return string.IsNullOrEmpty(tableIdentifier.SchemaName)
                ? $"[{tableIdentifier.TableName}]"
                : $"[{tableIdentifier.SchemaName}].[{tableIdentifier.TableName}]";
        }

        public override string ToSqliteTableName(TableIdentifier tableIdentifier)
        {
            if (string.IsNullOrEmpty(tableIdentifier.SchemaName) || tableIdentifier.SchemaName == "dbo")
                return tableIdentifier.TableName;

            return $"{tableIdentifier.SchemaName}_{tableIdentifier.TableName}";
        }

        public override void ValidateTableIdentifier(TableIdentifier tableIdentifier)
        {
            var regex = new Regex(@"^[\p{L}_][\p{L}\p{N}@$#_]{0,127}$");

            if (!regex.IsMatch(tableIdentifier.TableName))
                throw new DataliteException($"The provided table name, '{tableIdentifier.TableName}', is invalid.");

            if (!string.IsNullOrEmpty(tableIdentifier.SchemaName) && !regex.IsMatch(tableIdentifier.SchemaName))
                throw new DataliteException($"The provided schema name, '{tableIdentifier.SchemaName}', is invalid.");
        }
    }
}
