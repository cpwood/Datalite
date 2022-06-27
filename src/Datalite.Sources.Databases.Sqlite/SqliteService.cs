using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;
using Dapper;
using Datalite.Destination;
using Datalite.Sources.Databases.Shared;

namespace Datalite.Sources.Databases.Sqlite
{
    internal class SqliteService : DatabaseService
    {
        private readonly DbConnection _sourceConnection;

        public SqliteService(SqliteConnectionBroker sqliteConnection, DbConnection sourceConnection) 
            : base(sqliteConnection, sourceConnection)
        {
            _sourceConnection = sourceConnection;
        }

        public override bool CanGetAllTables => true;
        public override bool CanDiscoverTableIndexes => true;

        public override async Task<TableIdentifier[]> GetAllTablesAsync()
        {
            return (await _sourceConnection.QueryAsync<TableIdentifier>(@"
                SELECT      NULL AS SchemaName,
                            tbl_name AS TableName
                FROM        sqlite_master
                WHERE       type = 'table'")).ToArray();
        }

        public override async Task<TableDefinition> GetDefinitionFromSql(string sql, string outputTable)
        {
            // Don't return any data - we just want the schema back.
            // Wrap the query and add a WHERE clause that can never
            // be true to achieve this. This should return in milliseconds
            // regardless of the volume of data the original SQL would
            // return.
            sql = $"SELECT * FROM ({sql}) AS XXYYZZ WHERE 1 = 2";

            var cmd = _sourceConnection.CreateCommand();
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

        public override Task<TableDefinition> GetDefinitionFromTable(TableIdentifier tableIdentifier, string outputTable)
        {
            var sql = $"SELECT * FROM {ToTableName(tableIdentifier)}";
            return GetDefinitionFromSql(sql, outputTable);
        }

        public override Task<IDataReader> OpenReaderForTableAsync(TableIdentifier tableIdentifier)
        {
            return OpenReaderForQueryAsync($"SELECT * FROM {ToTableName(tableIdentifier)}");
        }

        public override async Task<IDataReader> OpenReaderForQueryAsync(string sql)
        {
            var cmd = _sourceConnection.CreateCommand();
            cmd.CommandText = sql;
            var reader = await cmd.ExecuteReaderAsync();
            return reader;
        }

        public override async Task<IEnumerable<string[]>> DiscoverTableIndexesAsync(TableIdentifier tableIdentifier)
        {
            var sql = @$"
                SELECT      'PK_{tableIdentifier.TableName}' AS Name,
                            'PrimaryKey' AS OriginalType,
                            name AS ColumnName,
                            cid + 1 AS ColumnOrder
                FROM        pragma_table_info('{tableIdentifier.TableName}')
                WHERE       pk = 1
                UNION
                SELECT      i.name AS Name,
                            'Index' AS OriginalType,
                            ic.name AS ColumnName,
                            ic.seqno + 1 AS ColumnOrder
                FROM        pragma_index_list('{tableIdentifier.TableName}') AS i
                CROSS JOIN  pragma_index_xinfo(i.name) AS ic
                WHERE       ic.name IS NOT NULL
                UNION
                SELECT      'FK_{tableIdentifier.TableName}_' || CAST(id AS TEXT) AS Name,
                            'ForeignKey' AS OriginalType,
                            [from] AS ColumnName,
                            seq + 1 AS ColumnOrder
                FROM        pragma_foreign_key_list('{tableIdentifier.TableName}')
                ORDER BY    1, 4";

            var candidates = await _sourceConnection.QueryAsync<IndexCandidate>(sql);

            return BuildIndexesFromCandidates(candidates);
        }

        public override string ToTableName(TableIdentifier tableIdentifier)
        {
            return $"\"{tableIdentifier.TableName}\"";
        }

        public override string ToSqliteTableName(TableIdentifier tableIdentifier)
        {
            return tableIdentifier.TableName;
        }

        public override void ValidateTableIdentifier(TableIdentifier tableIdentifier)
        {
            // Any character string is allowable when within quotes, so nothing can be invalid.
        }
    }
}
