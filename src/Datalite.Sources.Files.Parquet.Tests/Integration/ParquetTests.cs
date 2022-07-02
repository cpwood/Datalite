using System.IO;
using System.Reflection;
using Datalite.Destination;
using Datalite.Testing;
using Xunit;

namespace Datalite.Sources.Files.Parquet.Tests.Integration
{
    public class ParquetTests : TestBaseClass
    {
        private static string GetFullPath(string filename)
        {
            var dll = new FileInfo(Assembly.GetExecutingAssembly().Location);
            return Path.Combine(dll.DirectoryName!, "Integration", filename);
        }

        [Fact]
        public async void ReadsParquetSuccessfully()
        {
            await WithSqliteInMemoryConnection(async connection =>
            {
                await connection
                    .Add()
                    .FromParquet(GetFullPath("TestData.parquet"))
                    .AddIndex("id")
                    .AddIndex("last_name", "gender")
                    .ExecuteAsync();

                var table = await connection.LoadTableAsync("TestData");
                table
                    .Should()
                    .MeetTheTableConditions();
            });
        }
    }
}
