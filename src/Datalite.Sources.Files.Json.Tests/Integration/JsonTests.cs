using System.IO;
using System.Reflection;
using Datalite.Destination;
using Datalite.Testing;
using Xunit;

namespace Datalite.Sources.Files.Json.Tests.Integration
{
    public class JsonTests : TestBaseClass
    {
        private static string GetFullPath(string filename)
        {
            var dll = new FileInfo(Assembly.GetExecutingAssembly().Location);
            return Path.Combine(dll.DirectoryName!, "Integration", filename);
        }

        [Fact]
        public async void ReadsJsonSuccessfullyWithColumns()
        {
            await WithSqliteInMemoryConnection(async connection =>
            {
                await connection
                    .Add()
                    .FromJson(GetFullPath("TestData.json"), true)
                    .WithColumns(
                        new Column("id", typeof(int), true),
                        new Column("first_name", typeof(string), true),
                        new Column("last_name", typeof(string), true),
                        new Column("email", typeof(string), false),
                        new Column("gender", typeof(string), true),
                        new Column("image", typeof(byte[]), true, StringValueInterpretation.Base64),
                        new Column("salary", typeof(decimal), false)
                        )
                    .AddIndex("id")
                    .AddIndex("last_name", "gender")
                    .ExecuteAsync();

                var table = await connection.LoadTableAsync("TestData");
                table
                    .Should()
                    .MeetTheTableConditions();
            });
        }

        [Fact]
        public async void ReadsJsonSuccessfullyWithoutColumns()
        {
            await WithSqliteInMemoryConnection(async connection =>
            {
                await connection
                    .Add()
                    .FromJson(GetFullPath("TestData.json"), true)
                    .ExecuteAsync();

                var table = await connection.LoadTableAsync("TestData");
                table
                    .Should()
                    .Exist().And
                    .HaveAColumnCountOf(7).And
                    .HaveARowCountOf(1000);
            });
        }

        [Fact]
        public async void ReadsNestedJsonSuccessfullyWithSerialization()
        {
            await WithSqliteInMemoryConnection(async connection =>
            {
                await connection
                    .Add()
                    .FromJson(GetFullPath("NestedTestData.json"))
                    .ExecuteAsync();

                var table = await connection.LoadTableAsync("NestedTestData");
                table
                    .Should()
                    .Exist().And
                    .HaveAColumnCountOf(7).And
                    .HaveARowCountOf(2);
            });
        }

        [Fact]
        public async void ReadsNestedJsonSuccessfullyWithoutSerialization()
        {
            await WithSqliteInMemoryConnection(async connection =>
            {
                await connection
                    .Add()
                    .FromJson(GetFullPath("NestedTestData.json"))
                    .IgnoreNested()
                    .ExecuteAsync();

                var table = await connection.LoadTableAsync("NestedTestData");
                table
                    .Should()
                    .Exist().And
                    .HaveAColumnCountOf(5).And
                    .HaveARowCountOf(2);
            });
        }
    }
}
