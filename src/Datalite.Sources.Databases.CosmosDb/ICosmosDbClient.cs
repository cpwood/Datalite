using System.Collections.Generic;
using Microsoft.Azure.Cosmos;

namespace Datalite.Sources.Databases.CosmosDb
{
    /// <summary>
    /// Wrapper interface for CosmosDb access that facilitates unit testing.
    /// </summary>
    internal interface ICosmosDbClient
    {
        FeedIterator<Dictionary<string,object>> GetItemQueryIterator(string sql);
    }
}
