using Microsoft.Azure.Cosmos;

namespace Datalite.Sources.Databases.CosmosDb
{
    /// <summary>
    /// Connection details for a CosmosDb account.
    /// </summary>
    public class CosmosDbConnection
    {
        /// <summary>
        /// The URL for the CosmosDb account.
        /// </summary>
        public string Url { get; }

        /// <summary>
        /// The Access Key for the CosmosDb account.
        /// </summary>
        public string Key { get; }

        /// <summary>
        /// The database name.
        /// </summary>
        public string Database { get; }

        /// <summary>
        /// The container name.
        /// </summary>
        public string Container { get; }

        /// <summary>
        /// Options for the CosmosDb client.
        /// </summary>
        public CosmosClientOptions? Options { get; }

        /// <summary>
        /// Connection details for a CosmosDb account.
        /// </summary>
        /// <param name="url">The URL for the CosmosDb account.</param>
        /// <param name="key">The Access Key for the CosmosDb account.</param>
        /// <param name="database">The database name.</param>
        /// <param name="container">The container name.</param>
        /// <param name="options">Optional ComsmosDb client options.</param>
        public CosmosDbConnection(string url, string key, string database, string container, CosmosClientOptions? options = null)
        {
            Url = url;
            Key = key;
            Database = database;
            Container = container;
            Options = options;
        }
    }
}
