using Datalite.Destination;
using Datalite.Testing;
using Xunit;

namespace Datalite.Sources.Databases.SqlServer.Tests
{
    public class SqlServerTests : TestBaseClass, IClassFixture<SqlServerImage>
    {
        private readonly SqlServerImage _server;

        public SqlServerTests(SqlServerImage server)
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
                    .FromSqlServer(_server.GetConnectionString("Datalite"))
                    .FromTables()
                    .WithAutomaticIndexes()
                    .ExecuteAsync();

                var table = await connection.LoadTableAsync("TestData");
                table
                    .ShouldPassTableDataConditions();

                table = await connection.LoadTableAsync("Foo");
                table
                    .ShouldPassFooConditions();
            });
        }

        [Fact]
        public async void CopiesTablesScopedSuccessfully()
        {
            await WithSqliteInMemoryConnection(async connection =>
            {
                await connection
                    .Add()
                    .FromSqlServer(_server.GetConnectionString("Datalite"))
                    .FromTables("TestData")
                    .WithAutomaticIndexes()
                    .ExecuteAsync();

                var table = await connection.LoadTableAsync("TestData");
                table
                    .ShouldPassTableDataConditions();

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
                    .FromSqlServer(_server.GetConnectionString("Datalite"))
                    .FromTable("TestData")
                    .WithAutomaticIndexes()
                    .ExecuteAsync();

                var table = await connection.LoadTableAsync("TestData");
                table
                    .ShouldPassTableDataConditions();

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
                    .FromSqlServer(_server.GetConnectionString("Datalite"))
                    .FromQuery("SELECT id, first_name, last_name, email FROM TestData WHERE Id <= 3", "QueryData")
                    .AddIndex("email")
                    .ExecuteAsync();

                var table = await connection.LoadTableAsync("QueryData");
                table
                    .ShouldPassQueryDataConditions();

                table = await connection.LoadTableAsync("Foo");
                table
                    .Should()
                    .NotExist();
            });
        }
    }
}
