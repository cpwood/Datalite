using System.Data.SQLite;
using System.Reflection;
using Datalite.Destination;
using Datalite.Testing;
using Xunit;

namespace Datalite.Sources.Databases.Sqlite.Tests.Integration
{
    public class SystemSqliteTests : TestBaseClass
    {
        private static string BuildConnectionString()
        {
            var dll = new FileInfo(Assembly.GetExecutingAssembly().Location);
            return $"Data Source={Path.Combine(dll.DirectoryName!, "Integration", "test.sqlite")}";
        }

        private static async Task WithSystemDataSqliteInMemoryConnection(Func<SQLiteConnection, Task> action)
        {
            await using var connection = new SQLiteConnection("Data Source=:memory:");
            await connection.OpenAsync();
            await action(connection);
        }

        [Fact]
        public async void CopiesTablesSuccessfully()
        {
            await using var sourceConnection = new SQLiteConnection(BuildConnectionString());
            await sourceConnection.OpenAsync();

            await WithSystemDataSqliteInMemoryConnection(async connection =>
            {
                await connection
                    .Add()
                    .FromSqlite(sourceConnection)
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
            await using var sourceConnection = new SQLiteConnection(BuildConnectionString());
            await sourceConnection.OpenAsync();

            await WithSystemDataSqliteInMemoryConnection(async connection =>
            {
                await connection
                    .Add()
                    .FromSqlite(sourceConnection)
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
            await using var sourceConnection = new SQLiteConnection(BuildConnectionString());
            await sourceConnection.OpenAsync();

            await WithSystemDataSqliteInMemoryConnection(async connection =>
            {
                await connection
                    .Add()
                    .FromSqlite(sourceConnection)
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
            await using var sourceConnection = new SQLiteConnection(BuildConnectionString());
            await sourceConnection.OpenAsync();

            await WithSystemDataSqliteInMemoryConnection(async connection =>
            {
                await connection
                    .Add()
                    .FromSqlite(sourceConnection)
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