using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Abstractions;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Datalite.Destination;
using Datalite.Exceptions;
using Datalite.Sources.Databases.Shared;
using Datalite.Sources.Files.Json;
using Newtonsoft.Json.Linq;

namespace Datalite.Sources.Databases.H2
{
    internal class H2Service
    {
        private readonly H2Connection _h2Connection;
        private readonly SqliteConnectionBroker _sqliteConnection;
        private readonly IFileSystem _fs;
        private readonly IProcessRunner _processRunner;
        private static string? _javaDirectory;

        internal H2Service(
            H2Connection h2Connection, 
            SqliteConnectionBroker sqliteConnection,
            IFileSystem fs,
            IProcessRunner processRunner)
        {
            _h2Connection = h2Connection;
            _sqliteConnection = sqliteConnection;
            _fs = fs;
            _processRunner = processRunner;
        }

        /// <summary>
        /// Determine the path to the "java" folder where the folders containing the
        /// .jar and .class files are.
        /// </summary>
        internal string JavaDirectory
        {
            get
            {
                if (string.IsNullOrEmpty(_javaDirectory))
                {
                    var dll = _fs.FileInfo.FromFileName(System.Reflection.Assembly.GetExecutingAssembly().Location);

                    _javaDirectory = Path.Combine(
                        dll.DirectoryName!,
                        "java");
                }
                
                return _javaDirectory;
            }
        }

        internal string TemporaryWorkingDirectory => _fs.Path.GetTempPath();

        /// <summary>
        /// Run the provided SQL by invoking the Java process, passing the SQL, h2Connection information,
        /// job identifier and the path to the user's temporary folder.
        /// </summary>
        /// <param name="sql">The query to execute.</param>
        /// <returns>An object containing the path to the JSON description of the columns and the
        /// path to the JSONL file containing the actual data.</returns>
        /// <exception cref="DataliteException"></exception>
        internal async Task<QueryResult> RunQueryAsync(string sql)
        {
            sql = Regex.Replace(sql, @"\s+", " ").Trim();

            var jobId = Guid.NewGuid().ToString();

            var result = new QueryResult(_fs.Path.Combine(TemporaryWorkingDirectory, $"{jobId}_schema.json"),
                _fs.Path.Combine(TemporaryWorkingDirectory, $"{jobId}.json"));

            var psi = new ProcessStartInfo("java")
            {
                WorkingDirectory = JavaDirectory
            };

            var h2Jar = _h2Connection.Version == H2Connection.H2Version.Version1 ? "h2-1.4.200" : "h2-2.1.214";
            var separator = Environment.OSVersion.Platform == PlatformID.Win32NT ? ";" : ":";

            psi.Arguments =
                $"-cp jettison-1.5.0/jettison-1.5.0.jar{separator}{h2Jar}/{h2Jar}.jar{separator}datalite.h2.bridge.jar datalite.h2.bridge.Executor \"{_h2Connection.ConnectionString}\" \"{_h2Connection.Username}\" \"{_h2Connection.Password}\" \"{sql}\" \"{jobId}\" {TemporaryWorkingDirectory}";

            var output = await _processRunner.RunAsync(psi);

            if (!_fs.File.Exists(result.DataFilename))
            {
                // Schema file may exist even if the data doesn't..
                DeleteFiles(result);

                throw new DataliteException(
                    $"There was a problem interacting with Java. Please confirm that Java Runtime Environment version 8 or higher is installed and that you've chosen the correct H2 version. More details of the problem encountered can be found in the Data property of this exception.",
                    new Dictionary<string, object>
                    {
                    { "jobId", jobId },
                    { "sql", sql },
                    { "output", output },
                    { "workingDirectory", JavaDirectory },
                    { "arguments", psi.Arguments }
                    });
            }

            return result;
        }

        /// <summary>
        /// Helper method to read JSONL files and do something with the JObject found on each line.
        /// </summary>
        /// <param name="filename">The filename of the JSONL file.</param>
        /// <param name="resultAction">The action to take on the deserialised JObject.</param>
        /// <returns></returns>
        internal async Task ReadJsonlAsync(string filename, Action<JObject> resultAction)
        {
            using var reader = _fs.File.OpenText(filename);

            while (!reader.EndOfStream)
            {
                var line = await reader.ReadLineAsync();
                if (!string.IsNullOrEmpty(line))
                    resultAction(JObject.Parse(line));
            }
        }

        /// <summary>
        /// Discover all the user tables in the database.
        /// </summary>
        /// <returns>The list of user tables in the database.</returns>
        /// <exception cref="InvalidOperationException"></exception>
        public async Task<TableIdentifier[]> GetAllTablesAsync()
        {
            QueryResult? result = null;

            try
            {
                if (_h2Connection.Version == H2Connection.H2Version.Version1)
                {
                    result = await RunQueryAsync(@"
                        SELECT      TABLE_SCHEMA AS SchemaName,
                                    TABLE_NAME AS TableName
                        FROM        INFORMATION_SCHEMA.TABLES
                        WHERE       TABLE_TYPE = 'TABLE'
                        ORDER BY    TABLE_NAME");
                }
                else
                {
                    result = await RunQueryAsync(@"
                        SELECT      TABLE_SCHEMA AS SchemaName,
                                    TABLE_NAME AS TableName
                        FROM        INFORMATION_SCHEMA.TABLES
                        WHERE       TABLE_CLASS = 'org.h2.mvstore.db.MVTable'
                        ORDER BY    TABLE_NAME");
                }

                var tables = new List<TableIdentifier>();

                await ReadJsonlAsync(result.DataFilename,
                    x => tables.Add(x.ToObject<TableIdentifier>() ?? throw new InvalidOperationException()));

                return tables.ToArray();
            }
            finally
            {
                DeleteFiles(result);
            }
        }

        /// <summary>
        /// Find columns that are part of a primary key, foreign key or index so that
        /// the output table can receive equivalent indexes.
        /// </summary>
        /// <param name="tableIdentifier">The H2 table name.</param>
        /// <returns>Indexes to be applied to the Sqlite table.</returns>
        /// <exception cref="InvalidOperationException"></exception>
        public async Task<IEnumerable<string[]>> DiscoverTableIndexesAsync(TableIdentifier tableIdentifier)
        {
            QueryResult? result = null;

            try
            {
                var sql = _h2Connection.Version == H2Connection.H2Version.Version1 ? @$"
                    SELECT      INDEX_NAME AS Name,
                                INDEX_TYPE_NAME AS OriginalType,
                                COLUMN_NAME AS ColumnName,
                                ORDINAL_POSITION AS ColumnOrder
                    FROM        INFORMATION_SCHEMA.INDEXES
                    WHERE       TABLE_SCHEMA = '{tableIdentifier.SchemaName ?? "PUBLIC"}'
                    AND         TABLE_NAME = '{tableIdentifier.TableName}'
                    ORDER BY    1, 4" :
                    @$"
                    SELECT      INDEX_NAME AS Name,
                                'UNKNOWN' AS OriginalType,
                                COLUMN_NAME AS ColumnName,
                                ORDINAL_POSITION AS ColumnOrder
                    FROM        INFORMATION_SCHEMA.INDEX_COLUMNS
                    WHERE       TABLE_SCHEMA = '{tableIdentifier.SchemaName ?? "PUBLIC"}'
                    AND         TABLE_NAME = '{tableIdentifier.TableName}'
                    ORDER BY    1, 4";

                result = await RunQueryAsync(sql);

                var candidates = new List<IndexCandidate>();

                await ReadJsonlAsync(result.DataFilename,
                    x => candidates.Add(x.ToObject<IndexCandidate>() ?? throw new InvalidOperationException()));

                var grouped = candidates.GroupBy(x => x.Name, x => x.ColumnName, (n, c) => new
                {
                    Name = n,
                    Columns = c.ToArray()
                }).ToArray();

                var output = new List<string[]>();

                foreach (var g in grouped)
                {
                    if (g.Columns.Any() && !output.Any(x => x.SequenceEqual(g.Columns)))
                    {
                        output.Add(g.Columns!);
                    }
                }

                return output;
            }
            finally
            {
                DeleteFiles(result);
            }
        }

        /// <summary>
        /// Delete the temporary JSON(L) files, if they exist.
        /// </summary>
        /// <param name="result"></param>
        internal void DeleteFiles(QueryResult? result)
        {
            if (result == null) return;

            if (!string.IsNullOrEmpty(result.SchemaFilename) && _fs.File.Exists(result.SchemaFilename))
                _fs.File.Delete(result.SchemaFilename);

            if (!string.IsNullOrEmpty(result.DataFilename) && _fs.File.Exists(result.DataFilename))
                _fs.File.Delete(result.DataFilename);
        }

        /// <summary>
        /// Decide on the CLR types and Sqlite Storage Classes to use based on the information
        /// retrieved from the JDBC result set in the Java process.
        /// </summary>
        /// <param name="outputTable">The name of the output table.</param>
        /// <param name="result">The paths returned from the Java process.</param>
        /// <returns>The derived table definition.</returns>
        /// <exception cref="DataliteException"></exception>
        internal async Task<TableDefinition> GetDefinitionAsync(string outputTable, QueryResult result)
        {
            var columns = new Dictionary<string, Column>();
            var schema = JArray.Parse(await _fs.File.ReadAllTextAsync(result.SchemaFilename));

            // ReSharper disable once PossibleInvalidCastExceptionInForeachLoop
            // We control the JSON output, so we know it's an object.
            foreach (JObject col in schema)
            {
                var colName = col["column"]!.Value<string>()!;
                var colType = (JdbcTypes)col["type"]!.Value<int>();
                var required = col["required"]!.Value<bool>();

                switch (colType)
                {
                    case JdbcTypes.ARRAY:
                        columns.Add(colName,
                            new Column(colName, typeof(JArray), required));
                        break;
                    case JdbcTypes.BIGINT:
                        columns.Add(colName,
                            new Column(colName, typeof(long), required));
                        break;
                    case JdbcTypes.BINARY:
                    case JdbcTypes.BLOB:
                    case JdbcTypes.LONGVARBINARY:
                    case JdbcTypes.VARBINARY:
                        // Read it as a string but write it as binary. Our type converter will
                        // see a "base64:" prefix and Base64-decode the data back into a byte array.
                        columns.Add(colName,
                            new Column(colName, typeof(string),
                                StoragesClasses.StorageClassType.BlobClass, required));
                        break;
                    case JdbcTypes.BIT:
                    case JdbcTypes.BOOLEAN:
                        columns.Add(colName,
                            new Column(colName, typeof(bool), required));
                        break;
                    case JdbcTypes.CHAR:
                    case JdbcTypes.CLOB:
                    case JdbcTypes.LONGNVARCHAR:
                    case JdbcTypes.LONGVARCHAR:
                    case JdbcTypes.NCHAR:
                    case JdbcTypes.NCLOB:
                    case JdbcTypes.NVARCHAR:
                    case JdbcTypes.TIME:
                    case JdbcTypes.TIME_WITH_TIMEZONE:
                    case JdbcTypes.VARCHAR:
                        columns.Add(colName,
                            new Column(colName, typeof(string), required));
                        break;
                    case JdbcTypes.DATE:
                    case JdbcTypes.TIMESTAMP:
                    case JdbcTypes.TIMESTAMP_WITH_TIMEZONE:
                        columns.Add(colName,
                            new Column(colName, typeof(DateTime), required));
                        break;
                    case JdbcTypes.DECIMAL:
                        columns.Add(colName,
                            new Column(colName, typeof(decimal), required));
                        break;
                    case JdbcTypes.DOUBLE:
                        columns.Add(colName,
                            new Column(colName, typeof(double), required));
                        break;
                    case JdbcTypes.FLOAT:
                    case JdbcTypes.NUMERIC:
                    case JdbcTypes.REAL:
                        columns.Add(colName,
                            new Column(colName, typeof(float), required));
                        break;
                    case JdbcTypes.INTEGER:
                        columns.Add(colName,
                            new Column(colName, typeof(int), required));
                        break;
                    case JdbcTypes.SMALLINT:
                    case JdbcTypes.TINYINT:
                        columns.Add(colName,
                            new Column(colName, typeof(short), required));
                        break;
                    default:
                        throw new DataliteException($"Unable to handle a column of type {colType} for column {colName}");

                }
            }

            return new TableDefinition(outputTable)
            {
                Columns = columns
            };
        }

        public async Task ExecuteAsync(DatabaseDataliteContext context)
        {
            var opened = false;

            try
            {
                if (!_sqliteConnection.IsOpen)
                {
                    await _sqliteConnection.Connection.OpenAsync();
                    opened = true;
                }

                switch (context.Mode)
                {
                    case DatabaseDataliteContext.CommandType.Tables:
                        await ExecuteTablesAsync(context.Tables, context.AutoIndexes);
                        break;
                    case DatabaseDataliteContext.CommandType.Table:
                        await ExecuteTableAsync(context.Table!, context.OutputTable, context.AutoIndexes, context.Indexes.ToArray());
                        break;
                    case DatabaseDataliteContext.CommandType.Query:
                        await ExecuteQueryAsync(context.Sql, context.OutputTable, context.Indexes.ToArray());
                        break;
                }
            }
            finally
            {
                if (opened)
                    await _sqliteConnection.Connection.CloseAsync();
            }
        }

        internal async Task ExecuteTablesAsync(TableIdentifier[] tables, bool autoIndexes)
        {
            if (!tables.Any())
                tables = await GetAllTablesAsync();

            foreach (var table in tables)
            {
                await ExecuteTableAsync(table, ToSqliteTableName(table), autoIndexes, Array.Empty<string[]>());
            }
        }

        internal async Task ExecuteTableAsync(TableIdentifier tableIdentifier, string outputTable, bool autoIndexes, string[][] indexes)
        {
            QueryResult? result = null;

            if (string.IsNullOrEmpty(outputTable))
                outputTable = ToSqliteTableName(tableIdentifier);

            try
            {
                var sql = $"SELECT * FROM {ToTableName(tableIdentifier)}";
                result = await RunQueryAsync(sql);

                var tableDefinition = await GetDefinitionAsync(outputTable, result);

                var finalIndexes = new List<string[]>();

                if (autoIndexes)
                    finalIndexes.AddRange(await DiscoverTableIndexesAsync(tableIdentifier));

                foreach (var index in indexes)
                {
                    if (!indexes.Any(x => x.SequenceEqual(index)))
                        finalIndexes.Add(index);
                }

                var jsonBuilder = _sqliteConnection
                    .Connection
                    .Add()
                    .FromJson(result.DataFilename, outputTable, true, _fs)
                    .WithColumns(tableDefinition.Columns.Values.ToArray());

                foreach (var index in finalIndexes)
                {
                    jsonBuilder.AddIndex(index);
                }

                await jsonBuilder.ExecuteAsync();
            }
            finally
            {
                DeleteFiles(result);
            }
        }

        internal async Task ExecuteQueryAsync(string sql, string outputTable, string[][] indexes)
        {
            if (string.IsNullOrEmpty(sql))
                throw new DataliteException("A valid SQL query must be provided.");

            QueryResult? result = null;

            try
            {
                result = await RunQueryAsync(sql);

                var jsonBuilder = _sqliteConnection
                    .Connection
                    .Add()
                    .FromJson(result.DataFilename, outputTable, true, _fs);

                foreach (var index in indexes)
                {
                    jsonBuilder.AddIndex(index);
                }

                await jsonBuilder.ExecuteAsync();
            }
            finally
            {
                DeleteFiles(result);
            }
        }

        public string ToTableName(TableIdentifier tableIdentifier)
        {
            return string.IsNullOrEmpty(tableIdentifier.SchemaName)
                ? $"\"{tableIdentifier.TableName}\""
                : $"\"{tableIdentifier.SchemaName}\".\"{tableIdentifier.TableName}\"";
        }

        public string ToSqliteTableName(TableIdentifier tableIdentifier)
        {
            if (string.IsNullOrEmpty(tableIdentifier.SchemaName) || tableIdentifier.SchemaName == "PUBLIC")
                return tableIdentifier.TableName;

            return $"{tableIdentifier.SchemaName}_{tableIdentifier.TableName}";
        }
    }
}