using System.IO;
using System.Reflection;
using Datalite.Destination;
using Datalite.Testing;
using Xunit;

namespace Datalite.Sources.Databases.H2.Tests.Integration
{
    public class H2V1Tests : TestBaseClass
    {
        private static H2Connection BuildConnection()
        {
            var dll = new FileInfo(Assembly.GetExecutingAssembly().Location);
            var path = Path.Combine(dll.DirectoryName!, "Integration", "h2-1").Replace("\\", "/");

            return new H2Connection($"jdbc:h2:{path}", "foo", "bar", H2Connection.H2Version.Version1);

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
    }
}