using System.IO;
using System.Reflection;
using System.Threading.Tasks;
using Datalite.Destination;
using Datalite.Testing;
using Newtonsoft.Json.Linq;
using Xunit;

namespace Datalite.Sources.Databases.CosmosDb.Tests.Integration
{
    public class CosmosDbTestsWithFakes : TestBaseClass
    {
        private async Task<FakeCosmosDbClient> CreateFakeCosmosDbClientAsync()
        {
            var client = new FakeCosmosDbClient();
            var dll = new FileInfo(Assembly.GetExecutingAssembly().Location);
            var filename = Path.Combine(dll.DirectoryName!, "Integration", "TestData.json");

            await using var stream = File.OpenRead(filename);
            var reader = new StreamReader(stream);

            while (!reader.EndOfStream)
            {
                var line = await reader.ReadLineAsync();

                if (!string.IsNullOrEmpty(line))
                {
                    client.AddRecord(JObject.Parse(line));
                }
            }

            return client;
        }

        [Fact]
        public async void PerformsQuerySuccessfully()
        {
            await WithSqliteInMemoryConnection(async connection =>
            {
                await connection
                    .Add()
                    .FromCosmosDb(await this.CreateFakeCosmosDbClientAsync())
                    .FromQuery("SELECT * FROM c", "TestData")
                    .AddIndex("id")
                    .ExecuteAsync();

                var table = await connection.LoadTableAsync("TestData");

                table
                    .Should()
                    .MeetTheTableConditions();
            });
        }
    }
}
