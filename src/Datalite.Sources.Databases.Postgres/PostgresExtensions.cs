using Datalite.Exceptions;
using Datalite.Sources.Databases.Shared;
using Npgsql;

namespace Datalite.Sources.Databases.Postgres
{
    public static class PostgresExtensions
    {
        /// <summary>
        /// Load data from a Postgres data source.
        /// </summary>
        /// <param name="adc"></param>
        /// <param name="connectionString">The Postgres connection string for the data source.</param>
        /// <returns></returns>
        /// <exception cref="DataliteException"></exception>
        public static DatabaseCommand FromPostgres(this AddDataCommand adc, string connectionString)
        {
            if (string.IsNullOrEmpty(connectionString))
                throw new DataliteException("A valid Postgres connection string or NpgsqlConnection object must be provided.");

            var connection = new NpgsqlConnection(connectionString);
            var service = new PostgresService(adc.Connection, connection);
            var context = new DatabaseDataliteContext(true, ctx => service.ExecuteAsync(ctx));

            return new DatabaseCommand(context);
        }

        /// <summary>
        /// Load data from a Postgres data source.
        /// </summary>
        /// <param name="adc"></param>
        /// <param name="sqlConnection">An <see cref="NpgsqlConnection"/> object.</param>
        /// <returns></returns>
        /// <exception cref="DataliteException"></exception>
        public static DatabaseCommand FromPostgres(this AddDataCommand adc, NpgsqlConnection sqlConnection)
        {
            if (sqlConnection == null)
                throw new DataliteException("A valid Postgres connection string or NpgsqlConnection object must be provided.");

            var service = new PostgresService(adc.Connection, sqlConnection);
            var context = new DatabaseDataliteContext(false, ctx => service.ExecuteAsync(ctx));

            return new DatabaseCommand(context);
        }
    }
}