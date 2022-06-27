using System;
using System.Data.Common;
using Datalite.Exceptions;
using Datalite.Sources.Databases.Shared;
using Microsoft.Data.Sqlite;

namespace Datalite.Sources.Databases.Sqlite
{
    public static class SqliteExtensions
    {
        /// <summary>
        /// Load data from a Sqlite data source.
        /// </summary>
        /// <param name="adc"></param>
        /// <param name="connectionString">The Sqlite connection string for the data source.</param>
        /// <returns></returns>
        /// <exception cref="DataliteException"></exception>
        public static DatabaseCommand FromSqlite(this AddDataCommand adc, string connectionString)
        {
            if (string.IsNullOrEmpty(connectionString))
                throw new DataliteException("A valid Sqlite connection string, Microsoft.Data.Sqlite.SqliteConnection object or System.Data.SQLite.SQLiteConnection object must be provided.");

            var connection = new SqliteConnection(connectionString);
            var service = new SqliteService(adc.Connection, connection);
            var context = new DatabaseDataliteContext(true, ctx => service.ExecuteAsync(ctx));

            return new DatabaseCommand(context);
        }

        /// <summary>
        /// Load data from a Sqlite data source.
        /// </summary>
        /// <param name="adc"></param>
        /// <param name="sqlConnection">An <see cref="SqliteConnection"/> object.</param>
        /// <returns></returns>
        /// <exception cref="DataliteException"></exception>
        public static DatabaseCommand FromSqlite(this AddDataCommand adc, SqliteConnection sqlConnection)
        {
            if (sqlConnection == null)
                throw new DataliteException("A valid Sqlite connection string, Microsoft.Data.Sqlite.SqliteConnection object or System.Data.SQLite.SQLiteConnection object must be provided.");

            var service = new SqliteService(adc.Connection, sqlConnection);
            var context = new DatabaseDataliteContext(false, ctx => service.ExecuteAsync(ctx));

            return new DatabaseCommand(context);
        }

        /// <summary>
        /// Load data from a Sqlite data source.
        /// </summary>
        /// <param name="adc"></param>
        /// <param name="sqlConnection">A <see cref="DbConnection"/> object. Must be an Microsoft.Data.Sqlite.SqliteConnection object or System.Data.SQLite.SQLiteConnection.</param>
        /// <returns></returns>
        /// <exception cref="DataliteException"></exception>
        public static DatabaseCommand FromSqlite(this AddDataCommand adc, DbConnection sqlConnection)
        {
            if (sqlConnection == null)
                throw new DataliteException("A valid Sqlite connection string, Microsoft.Data.Sqlite.SqliteConnection object or System.Data.SQLite.SQLiteConnection object must be provided.");

            if (sqlConnection.GetType() != typeof(SqliteConnection) &&
                sqlConnection.GetType().FullName != "System.Data.SQLite.SQLiteConnection")
            {
                throw new ArgumentOutOfRangeException(nameof(sqlConnection),
                    "The connection must either be a Microsoft.Data.Sqlite.SqliteConnection or a System.Data.SQLite.SQLiteConnection");
            }

            var service = new SqliteService(adc.Connection, sqlConnection);
            var context = new DatabaseDataliteContext(false, ctx => service.ExecuteAsync(ctx));

            return new DatabaseCommand(context);
        }
    }
}