using System.Data.Odbc;
using Datalite.Exceptions;
using Datalite.Sources.Databases.Shared;

namespace Datalite.Sources.Databases.Odbc
{
    public static class OdbcExtensions
    {
        /// <summary>
        /// Load data from an ODBC data source.
        /// </summary>
        /// <param name="adc"></param>
        /// <param name="connectionString">The ODBC connection string for the data source.</param>
        /// <returns></returns>
        /// <exception cref="DataliteException"></exception>
        public static DatabaseCommand FromOdbc(this AddDataCommand adc, string connectionString)
        {
            if (string.IsNullOrEmpty(connectionString))
                throw new DataliteException("A valid ODBC connection string or OdbcConnection object must be provided.");

            var connection = new OdbcConnection(connectionString);
            var service = new OdbcService(adc.Connection, connection);
            var context = new DatabaseDataliteContext(true, ctx => service.ExecuteAsync(ctx));

            return new DatabaseCommand(context);
        }

        /// <summary>
        /// Load data from an ODBC data source.
        /// </summary>
        /// <param name="adc"></param>
        /// <param name="sqlConnection">An <see cref="OdbcConnection"/> object.</param>
        /// <returns></returns>
        /// <exception cref="DataliteException"></exception>
        public static DatabaseCommand FromOdbc(this AddDataCommand adc, OdbcConnection sqlConnection)
        {
            if (sqlConnection == null)
                throw new DataliteException("A valid ODBC connection string or OdbcConnection object must be provided.");

            var service = new OdbcService(adc.Connection, sqlConnection);
            var context = new DatabaseDataliteContext(false, ctx => service.ExecuteAsync(ctx));

            return new DatabaseCommand(context);
        }
    }
}