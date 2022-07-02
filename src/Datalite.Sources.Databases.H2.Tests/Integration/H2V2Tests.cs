using System.IO;
using System.Reflection;
using Datalite.Destination;
using Datalite.Sources.Databases.Shared;
using Datalite.Testing;
using Xunit;

namespace Datalite.Sources.Databases.H2.Tests.Integration
{
    [Collection("H2")]
    public class H2V2Tests : TestBaseClass
    {
        private static H2Connection BuildConnection()
        {
            var dll = new FileInfo(Assembly.GetExecutingAssembly().Location);
            var path = Path.Combine(dll.DirectoryName!, "Integration", "h2-2").Replace("\\", "/");

            return new H2Connection($"jdbc:h2:{path}", "foo", "bar");

        }

        [Fact]
        public async void CopiesTablesSuccessfully()
        {
            await WithSqliteInMemoryConnection(async connection =>
            {
                await connection
                    .Add()
                    .FromH2(BuildConnection())
                    .FromTables()
                    .WithAutomaticIndexes()
                    .ExecuteAsync();

                var table = await connection.LoadTableAsync("TESTDATA");
                table
                    .Should()
                    .MeetTheTableConditions();

                table = await connection.LoadTableAsync("FOO");
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
                    .FromH2(BuildConnection())
                    .FromTables("TESTDATA")
                    .WithAutomaticIndexes()
                    .ExecuteAsync();

                var table = await connection.LoadTableAsync("TESTDATA");
                table
                    .Should()
                    .MeetTheTableConditions();

                table = await connection.LoadTableAsync("FOO");
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
                    .FromH2(BuildConnection())
                    .FromTable("TESTDATA")
                    .WithAutomaticIndexes()
                    .ExecuteAsync();

                var table = await connection.LoadTableAsync("TESTDATA");
                table
                    .Should()
                    .MeetTheTableConditions();

                table = await connection.LoadTableAsync("FOO");
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
                    .FromH2(BuildConnection())
                    .FromQuery("SELECT id, first_name, last_name, email FROM TESTDATA WHERE Id <= 3", "QUERYDATA")
                    .AddIndex("email")
                    .ExecuteAsync();

                var table = await connection.LoadTableAsync("QUERYDATA");
                table
                    .Should()
                    .MeetTheQueryConditions();

                table = await connection.LoadTableAsync("FOO");
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
                    .FromH2(BuildConnection())
                    .FromTable(new TableIdentifier("TEST", "TESTVALUES"))
                    .WithAutomaticIndexes()
                    .ExecuteAsync();

                var table = await connection.LoadTableAsync("TEST_TESTVALUES");
                table
                    .Should()
                    .Exist().And
                    .HaveAColumnCountOf(2).And
                    .HaveARowCountOf(1);
            });
        }
    }
}