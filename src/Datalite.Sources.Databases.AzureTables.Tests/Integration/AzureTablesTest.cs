using System.Threading.Tasks;
using Datalite.Destination;
using Datalite.Testing;
using Xunit;

namespace Datalite.Sources.Databases.AzureTables.Tests.Integration
{
    public class AzureTablesTests : TestBaseClass, IClassFixture<AzuriteImage>
    {
        private readonly AzuriteImage _server;

        public AzureTablesTests(AzuriteImage server)
        {
            _server = server;
        }

        [Fact]
        public async Task CopiesTablesSuccessfully()
        {
            await WithSqliteInMemoryConnection(async connection =>
            {
                await connection
                    .Add()
                    .FromAzureTables(_server.GetConnectionString())
                    .FromTables()
                    .ExecuteAsync();

                var table = await connection.LoadTableAsync("TestData");

                table
                    .Should()
                    .MeetTheTableConditions();

                table = await connection.LoadTableAsync("Foo");
                table
                    .Should()
                    .MeetTheExtraTableConditions(3);
            });
        }

        [Fact]
        public async Task CopiesTablesScopedSuccessfully()
        {
            await WithSqliteInMemoryConnection(async connection =>
            {
                await connection
                    .Add()
                    .FromAzureTables(_server.GetConnectionString())
                    .FromTables("TestData")
                    .ExecuteAsync();

                var table = await connection.LoadTableAsync("TestData");

                table
                    .Should()
                    .MeetTheTableConditions();

                table = await connection.LoadTableAsync("Foo");
                table
                    .Should()
                    .NotExist();
            });
        }

        [Fact]
        public async Task CopiesTableSuccessfully()
        {
            await WithSqliteInMemoryConnection(async connection =>
            {
                await connection
                    .Add()
                    .FromAzureTables(_server.GetConnectionString())
                    .FromTable("TestData")
                    .ExecuteAsync();

                var table = await connection.LoadTableAsync("TestData");

                table
                    .Should()
                    .MeetTheTableConditions();

                table = await connection.LoadTableAsync("Foo");
                table
                    .Should()
                    .NotExist();
            });
        }

        [Fact]
        public async Task PerformsQuerySuccessfully()
        {
            await WithSqliteInMemoryConnection(async connection =>
            {
                await connection
                    .Add()
                    .FromAzureTables(_server.GetConnectionString())
                    .FromTable("TestData")
                    .ToTable("QueryData")
                    .WithFilter("id lt 4")
                    .AddIndex("email")
                    .ExecuteAsync();

                var table = await connection.LoadTableAsync("QueryData");
                table
                    .Should()
                    .MeetTheQueryConditions();

                table = await connection.LoadTableAsync("Foo");
                table
                    .Should()
                    .NotExist();
            });

        }

        [Fact]
        public async Task CopiesTableWithColumnsSuccessfully()
        {
            await WithSqliteInMemoryConnection(async connection =>
            {
                await connection
                    .Add()
                    .FromAzureTables(_server.GetConnectionString())
                    .FromTable("Foo")
                    .WithColumns(
                        new Column("PartitionKey", typeof(string), true),
                        new Column("RowKey", typeof(string), true),
                        new Column("Timestamp", typeof(string), true)
                    )
                    .ToTable("Bar")
                    .ExecuteAsync();

                var table = await connection.LoadTableAsync("Bar");
                table
                    .Should()
                    .MeetTheExtraTableConditions(3);
            });
        }
    }
}
