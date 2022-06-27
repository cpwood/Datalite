using System.Collections.Generic;
using Microsoft.Azure.Cosmos;

namespace Datalite.Sources.Databases.CosmosDb
{
    /// <summary>
    /// Wrapper class for CosmosDb access that facilitates unit testing.
    /// </summary>
    internal class CosmosDbClient : ICosmosDbClient
    {
        private readonly Container _container;

        public CosmosDbClient(CosmosDbConnection connection)
        {
            var client = new CosmosClient(connection.Url, connection.Key);
            _container = client.GetContainer(connection.Database, connection.Container);
        }

        public FeedIterator<Dictionary<string,object>> GetItemQueryIterator(string sql)
        {
            var query = new QueryDefinition(sql);
            return _container.GetItemQueryIterator<Dictionary<string, object>>(query);
        }
    }
}
