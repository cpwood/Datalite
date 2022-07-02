using Datalite.Exceptions;

namespace Datalite.Sources.Databases.CosmosDb
{
    /// <summary>
    /// Cosmos DB Extensions
    /// </summary>
    public static class CosmosDbExtensions
    {
        /// <summary>
        /// Load data from a CosmsoDb account.
        /// </summary>
        /// <param name="adc"></param>
        /// <param name="connection">The CosmosDb account connection details.</param>
        /// <returns></returns>
        /// <exception cref="DataliteException"></exception>
        public static CosmosDbCommand FromCosmosDb(this AddDataCommand adc, CosmosDbConnection connection)
        {
            if (string.IsNullOrEmpty(connection.Url))
                throw new DataliteException("A CosmosDb URL must be provided.");

            if (string.IsNullOrEmpty(connection.Key))
                throw new DataliteException("A CosmosDb key must be provided.");

            if (string.IsNullOrEmpty(connection.Database))
                throw new DataliteException("A database name must be provided.");

            if (string.IsNullOrEmpty(connection.Container))
                throw new DataliteException("A container name must be provided.");

            var client = new CosmosDbClient(connection);
            var service = new CosmosDbService(adc.Connection, client);
            var context = new CosmosDbDataliteContext(ctx => service.ExecuteAsync(ctx));

            return new CosmosDbCommand(context);
        }

        internal static CosmosDbCommand FromCosmosDb(this AddDataCommand adc, FakeCosmosDbClient client)
        {
            var service = new CosmosDbService(adc.Connection, client);
            var context = new CosmosDbDataliteContext(ctx => service.ExecuteAsync(ctx));

            return new CosmosDbCommand(context);
        }
    }
}