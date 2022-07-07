using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json.Linq;

namespace Datalite.Sources.Databases.CosmosDb.Tests
{
    internal class FakeCosmosDbClient : ICosmosDbClient
    {
        private readonly List<Dictionary<string, object>> _records = new();

        public void AddRecord(JObject obj)
        {
            var dictionary = new Dictionary<string, object>(
                obj.Properties()
                    .Where(x => x.Value<object>() != null)
                    .Select(x => new KeyValuePair<string, object>(x.Name, x.Value.ToObject<object>()!)));
            
            if (!dictionary.ContainsKey("_ts"))
                dictionary.Add("_ts", 1656767482);
            
            _records.Add(dictionary);
        }

        public FeedIterator<Dictionary<string, object>> GetItemQueryIterator(string sql)
        {
            return new FakeFeedIterator(_records);
        }
    }

    internal class FakeFeedIterator : FeedIterator<Dictionary<string, object>>
    {
        private readonly List<Dictionary<string, object>> _records;
        private bool _moreResults = true;

        public FakeFeedIterator(List<Dictionary<string, object>> records)
        {
            _records = records;
        }

        public override Task<FeedResponse<Dictionary<string, object>>> ReadNextAsync(CancellationToken cancellationToken = new CancellationToken())
        {
            _moreResults = false;
            return Task.FromResult((FeedResponse<Dictionary<string,object>>) new FakeFeedResponse(_records));
        }

        public override bool HasMoreResults => _moreResults;
    }

    internal class FakeFeedResponse : FeedResponse<Dictionary<string, object>>
    {
        private readonly List<Dictionary<string, object>> _records;

        public FakeFeedResponse(List<Dictionary<string, object>> records)
        {
            _records = records;
        }

        public override Headers Headers { get; } = new Headers();
        public override IEnumerable<Dictionary<string, object>> Resource => _records;
        public override HttpStatusCode StatusCode => HttpStatusCode.OK;
        public override CosmosDiagnostics? Diagnostics => null;
        public override IEnumerator<Dictionary<string, object>> GetEnumerator() => _records.GetEnumerator();

        public override string? ContinuationToken => null;
        public override int Count => _records.Count;
        public override string? IndexMetrics => null;
    }
}
