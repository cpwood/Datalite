using System.Reflection;
using Datalite.Destination;
using Datalite.Testing;
using Xunit;

namespace Datalite.Sources.Databases.Sqlite.Tests.Integration
{
    public class MicrosoftSqliteTests : TestBaseClass
    {
        private static string BuildConnectionString()
        {
            var dll = new FileInfo(Assembly.GetExecutingAssembly().Location);
            return $"Data Source={Path.Combine(dll.DirectoryName!, "Integration", "test.sqlite")}";
        }

        [Fact]
        public async void CopiesTablesSuccessfully()
        {
            await WithSqliteInMemoryConnection(async connection =>
            {
                await connection
                    .Add()
                    .FromSqlite(BuildConnectionString())
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
                    .FromSqlite(BuildConnectionString())
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
                    .FromSqlite(BuildConnectionString())
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
                    .FromSqlite(BuildConnectionString())
                    .FromQuery("SELECT id, first_name, last_name, email FROM TestData WHERE Id <= 3", "QueryData")
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
    }
}