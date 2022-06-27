using System.Data.SqlClient;
using Datalite.Exceptions;
using Datalite.Sources.Databases.Shared;

namespace Datalite.Sources.Databases.SqlServer
{
    public static class SqlServerExtensions
    {
        /// <summary>
        /// Load data from a SQL Server data source.
        /// </summary>
        /// <param name="adc"></param>
        /// <param name="connectionString">The SQL Server connection string for the data source.</param>
        /// <returns></returns>
        /// <exception cref="DataliteException"></exception>
        public static DatabaseCommand FromSqlServer(this AddDataCommand adc, string connectionString)
        {
            if (string.IsNullOrEmpty(connectionString))
                throw new DataliteException("A valid SQL Server connection string or SqlConnection object must be provided.");

            var connection = new SqlConnection(connectionString);
            var service = new SqlServerService(adc.Connection, connection);
            var context = new DatabaseDataliteContext(true, ctx => service.ExecuteAsync(ctx));

            return new DatabaseCommand(context);
        }

        /// <summary>
        /// Load data from a SQL Server data source.
        /// </summary>
        /// <param name="adc"></param>
        /// <param name="sqlConnection">An <see cref="SqlConnection"/> object.</param>
        /// <returns></returns>
        /// <exception cref="DataliteException"></exception>
        public static DatabaseCommand FromSqlServer(this AddDataCommand adc, SqlConnection sqlConnection)
        {
            if (sqlConnection == null)
                throw new DataliteException("A valid SQL Server connection string or SqlConnection object must be provided.");

            var service = new SqlServerService(adc.Connection, sqlConnection);
            var context = new DatabaseDataliteContext(false, ctx => service.ExecuteAsync(ctx));

            return new DatabaseCommand(context);
        }
    }
}