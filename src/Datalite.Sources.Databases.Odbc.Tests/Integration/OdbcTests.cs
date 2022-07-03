using System.IO;
using System.Reflection;
using Datalite.Destination;
using Datalite.Testing;

namespace Datalite.Sources.Databases.Odbc.Tests.Integration
{
    public class OdbcTests : TestBaseClass
    {
        private static string BuildConnectionString()
        {
            var dll = new FileInfo(Assembly.GetExecutingAssembly().Location);
            var file = Path.Combine(dll.DirectoryName!, "Integration", "Test.accdb");
            return $"Driver={{Microsoft Access Driver (*.mdb, *.accdb)}};Dbq={file}";
        }
        

        [WindowsOnlyFact]
        public async void CopiesTableSuccessfully()
        {
            await WithSqliteInMemoryConnection(async connection =>
            {
                await connection
                    .Add()
                    .FromOdbc(BuildConnectionString())
                    .FromTable("TestData")
                    .AddIndex("id")
                    .AddIndex("last_name", "gender")
                    .ExecuteAsync();

                var table = await connection.LoadTableAsync("TestData");
                table
                    .Should()
                    .MeetTheTableConditions();
            });
        }

        [WindowsOnlyFact]
        public async void PerformsQuerySuccessfully()
        {
            await WithSqliteInMemoryConnection(async connection =>
            {
                await connection
                    .Add()
                    .FromOdbc(BuildConnectionString())
                    .FromQuery("SELECT id, first_name, last_name, email FROM TestData WHERE Id <= 3", "QueryData")
                    .AddIndex("email")
                    .ExecuteAsync();

                var table = await connection.LoadTableAsync("QueryData");
                table
                    .Should()
                    .MeetTheQueryConditions();
            });
        }
    }
}
