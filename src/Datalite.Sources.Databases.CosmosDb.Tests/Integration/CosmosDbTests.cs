using Datalite.Destination;
using Datalite.Testing;
using Xunit;

namespace Datalite.Sources.Databases.CosmosDb.Tests.Integration
{
    public class CosmosDbTests : TestBaseClass, IClassFixture<CosmosDbImage>
    {
        public CosmosDbTests(CosmosDbImage server)
        {
            var _ = server;
        }

        // TODO: reintroduce once the CosmosDb Emulator Docker image is stable enough.
        // At the time of writing it, fails far too often and for no good reason with 408 and 503 codes.
        //[Fact]
        public async void PerformsQuerySuccessfully()
        {
            await WithSqliteInMemoryConnection(async connection =>
            {
                await connection
                    .Add()
                    .FromCosmosDb(new CosmosDbConnection(CosmosDbImage.Url, CosmosDbImage.Key, "UnitTests", "MyData"))
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
