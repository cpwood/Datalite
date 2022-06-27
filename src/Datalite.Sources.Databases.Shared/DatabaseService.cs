using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;
using Datalite.Destination;
using Datalite.Exceptions;

namespace Datalite.Sources.Databases.Shared
{
    /// <summary>
    /// Provides common functionality for all ADO.NET database types.
    /// </summary>
    public abstract class DatabaseService
    {
        /// <summary>
        /// The Sqlite connection.
        /// </summary>
        protected SqliteConnectionBroker SqliteConnection { get; }

        /// <summary>
        /// The data source connection.
        /// </summary>
        protected DbConnection SourceConnection { get; }

        protected DatabaseService(
            SqliteConnectionBroker sqliteConnection,
            DbConnection sourceConnection)
        {
            SqliteConnection = sqliteConnection;
            SourceConnection = sourceConnection;
        }

        /// <summary>
        /// Whether it is possible to discover a list of tables from this data source.
        /// </summary>
        public abstract bool CanGetAllTables { get; }

        /// <summary>
        /// Whether it is possible to discover table indexes from this data source.
        /// </summary>
        public abstract bool CanDiscoverTableIndexes { get; }

        /// <summary>
        /// Gets an array of table names in the data source (if this operation is supported).
        /// </summary>
        /// <returns>An array of table names.</returns>
        /// <exception cref="NotSupportedException"></exception>
        public abstract Task<TableIdentifier[]> GetAllTablesAsync();

        /// <summary>
        /// Creates a <see cref="TableDefinition"/> using metadata from the data source.
        /// </summary>
        /// <param name="sql">The SQL query.</param>
        /// <param name="outputTable">The name of the Sqlite destination table.</param>
        /// <returns></returns>
        public abstract Task<TableDefinition> GetDefinitionFromSql(string sql, string outputTable);

        /// <summary>
        /// Creates a <see cref="TableDefinition"/> using metadata from the database server.
        /// </summary>
        /// <param name="tableIdentifier">The table in the data source.</param>
        /// <param name="outputTable">The name of the Sqlite destination table.</param>
        /// <returns></returns>
        public abstract Task<TableDefinition> GetDefinitionFromTable(TableIdentifier tableIdentifier, string outputTable);

        /// <summary>
        /// Opens an <see cref="IDataReader"/> on a table at the data source.
        /// </summary>
        /// <param name="tableIdentifier">The table name.</param>
        /// <returns>An <see cref="IDataReader"/> containing the table content.</returns>
        public abstract Task<IDataReader> OpenReaderForTableAsync(TableIdentifier tableIdentifier);

        /// <summary>
        /// Opens an <see cref="IDataReader"/> on the result set for a SQL query at the data source.
        /// </summary>
        /// <param name="sql">The SQL query.</param>
        /// <returns>An <see cref="IDataReader"/> containing the result set content.</returns>
        public abstract Task<IDataReader> OpenReaderForQueryAsync(string sql);

        /// <summary>
        /// Builds a list of indexes to be applied to the destination table by examining the source
        /// table's primary keys, foreign keys and indexes (where supported).
        /// </summary>
        /// <param name="tableIdentifier">The source table name.</param>
        /// <returns>An enumerable of string arrays. Each string array contains one more columns that will act as a composite index. </returns>
        public abstract Task<IEnumerable<string[]>> DiscoverTableIndexesAsync(TableIdentifier tableIdentifier);

        /// <summary>
        /// Turns a source table name - including the schema name - into a fully-qualified table name.
        /// </summary>
        /// <param name="tableIdentifier">The source table identifier.</param>
        /// <returns>A fully-qualified source table name.</returns>
        public abstract string ToTableName(TableIdentifier tableIdentifier);

        /// <summary>
        /// Turns a source table name - including the schema name - into a Sqlite table name.
        /// </summary>
        /// <param name="tableIdentifier">The source table identifier.</param>
        /// <returns>A Sqlite table name.</returns>
        public abstract string ToSqliteTableName(TableIdentifier tableIdentifier);

        /// <summary>
        /// Checks that a provided table identifier is valid for the source database system.
        /// </summary>
        /// <param name="tableIdentifier">The table identifier</param>
        /// <exception cref="DataliteException"></exception>
        public abstract void ValidateTableIdentifier(TableIdentifier tableIdentifier);

        /// <summary>
        /// Utility method that aggregates <see cref="IndexCandidate" /> records by name and returns a distinct
        /// list of indexes to be applied to the destination table.
        /// </summary>
        /// <param name="candidates"></param>
        /// <returns></returns>
        protected string[][] BuildIndexesFromCandidates(IEnumerable<IndexCandidate>? candidates)
        {
            var output = new List<string[]>();

            if (candidates == null)
                return output.ToArray();

            var grouped = candidates.GroupBy(x => x.Name, x => x.ColumnName, (n, c) => new
            {
                Name = n,
                Columns = c.ToArray()
            }).ToArray();

            foreach (var g in grouped)
            {
                if (g.Columns.Any() && !output.Any(x => x.SequenceEqual(g.Columns)))
                {
                    output.Add(g.Columns!);
                }
            }

            return output.ToArray();
        }

        /// <summary>
        /// Execute the data migration.
        /// </summary>
        /// <param name="context">The values collected through the configuration process.</param>
        /// <returns></returns>
        public async Task ExecuteAsync(DatabaseDataliteContext context)
        {
            var opened = false;

            try
            {
                if (context.CreatedConnection)
                    await SourceConnection.OpenAsync();

                if (!SqliteConnection.IsOpen)
                {
                    await SqliteConnection.Connection.OpenAsync();
                    opened = true;
                }

                switch (context.Mode)
                {
                    case DatabaseDataliteContext.CommandType.Tables:
                        await ExecuteTablesAsync(context.Tables, context.AutoIndexes);
                        break;
                    case DatabaseDataliteContext.CommandType.Table:
                        await ExecuteTableAsync(context.Table!, context.OutputTable,
                            context.Indexes.ToArray(), context.AutoIndexes);
                        break;
                    case DatabaseDataliteContext.CommandType.Query:
                        await ExecuteQueryAsync(context.Sql, context.OutputTable,
                            context.Indexes.ToArray());
                        break;
                }
            }
            finally
            {
                if (context.CreatedConnection)
                {
                    await SourceConnection.CloseAsync();
                    await SourceConnection.DisposeAsync();
                }

                if (opened)
                    await SqliteConnection.Connection.CloseAsync();
            }
        }

        private async Task ExecuteTableAsync(
            TableIdentifier table,
            string outputTable,
            string[][] indexes,
            bool autoIndexes)
        {
            ValidateTableIdentifier(table);

            if (string.IsNullOrEmpty(outputTable))
                outputTable = ToSqliteTableName(table);

            var definition = await GetDefinitionFromTable(table, outputTable);
            await SqliteConnection.CreateTableAsync(definition);

            using (var reader = await OpenReaderForTableAsync(table))
            {
                await SqliteConnection.FromDataReaderAsync(definition, reader);
            }

            var finalIndexes = new List<string[]>(indexes);

            if (CanDiscoverTableIndexes && autoIndexes)
            {
                var discoveredIndexes = await DiscoverTableIndexesAsync(table);

                foreach (var index in discoveredIndexes)
                {
                    if (!indexes.Any(x => x.SequenceEqual(index)))
                        finalIndexes.Add(index);
                }
            }

            foreach (var index in finalIndexes)
            {
                await SqliteConnection.CreateIndexAsync(outputTable, index);
            }
        }

        private async Task ExecuteTablesAsync(
            TableIdentifier[] tables,
            bool autoIndexes
            )
        {
            if (!CanGetAllTables && !tables.Any())
                throw new DataliteException("You must specify a list of tables for this type of connection");

            tables = tables.Any() ? tables : await GetAllTablesAsync();

            foreach (var table in tables)
            {
                await ExecuteTableAsync(table, ToSqliteTableName(table), Array.Empty<string[]>(), autoIndexes);
            }
        }

        private async Task ExecuteQueryAsync(
            string sql,
            string outputTable,
            string[][] indexes)
        {
            var definition = await GetDefinitionFromSql(sql, outputTable);
            await SqliteConnection.CreateTableAsync(definition);

            using (var reader = await OpenReaderForQueryAsync(sql))
            {
                await SqliteConnection.FromDataReaderAsync(definition, reader);
            }

            foreach (var index in indexes)
            {
                await SqliteConnection.CreateIndexAsync(outputTable, index);
            }
        }
    }
}
