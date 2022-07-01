using Datalite.Destination;
using Datalite.Sources.Databases.Shared;
using Datalite.Testing;
using Xunit;

namespace Datalite.Sources.Databases.Postgres.Tests.Integration
{
    public class PostgresTests : TestBaseClass, IClassFixture<PostgresImage>
    {
        private readonly PostgresImage _server;

        public PostgresTests(PostgresImage server)
        {
            _server = server;
        }

        [Fact]
        public async void CopiesTablesSuccessfully()
        {
            await WithSqliteInMemoryConnection(async connection =>
            {

                await connection
                    .Add()
                    .FromPostgres(_server.GetConnectionString("Datalite"))
                    .FromTables()
                    .WithAutomaticIndexes()
                    .ExecuteAsync();

                var table = await connection.LoadTableAsync("TestData");
                table
                    .Should()
                    .MeetTheTableConditions();

                table = await connection.LoadTableAsync("Foo");
                table
                    .Should()
                    .MeetTheExtraTableConditions();
            });
        }

        [Fact]
        public async void CopiesTablesScopedSuccessfully()
        {
            await WithSqliteInMemoryConnection(async connection =>
            {
                await connection
                    .Add()
                    .FromPostgres(_server.GetConnectionString("Datalite"))
                    .FromTables("TestData")
                    .WithAutomaticIndexes()
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
        public async void CopiesTableSuccessfully()
        {
            await WithSqliteInMemoryConnection(async connection =>
            {
                await connection
                    .Add()
                    .FromPostgres(_server.GetConnectionString("Datalite"))
                    .FromTable("TestData")
                    .WithAutomaticIndexes()
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
        public async void PerformsQuerySuccessfully()
        {
            await WithSqliteInMemoryConnection(async connection =>
            {
                await connection
                    .Add()
                    .FromPostgres(_server.GetConnectionString("Datalite"))
                    .FromQuery("SELECT id, first_name, last_name, email FROM \"TestData\" WHERE Id <= 3", "QueryData")
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
        public async void CopiesSchemaTableSuccessfully()
        {
            await WithSqliteInMemoryConnection(async connection =>
            {
                await connection
                    .Add()
                    .FromPostgres(_server.GetConnectionString("Datalite"))
                    .FromTable(new TableIdentifier("TEST", "TestValues"))
                    .WithAutomaticIndexes()
                    .ExecuteAsync();

                var table = await connection.LoadTableAsync("TEST_TestValues");
                table
                    .Should()
                    .Exist().And
                    .HaveAColumnCountOf(2).And
                    .HaveARowCountOf(1);
            });
        }
    }
}
