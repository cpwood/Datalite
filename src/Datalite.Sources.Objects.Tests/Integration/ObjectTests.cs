using System.Collections.Generic;
using System.IO;
using System.Reflection;
using System.Threading.Tasks;
using Datalite.Destination;
using Datalite.Testing;
using Newtonsoft.Json.Linq;
using Xunit;

namespace Datalite.Sources.Objects.Tests.Integration
{
    public class ObjectTests : TestBaseClass
    {
        private static string GetFullPath(string filename)
        {
            var dll = new FileInfo(Assembly.GetExecutingAssembly().Location);
            return Path.Combine(dll.DirectoryName!, "Integration", filename);
        }

        private static async Task<object[]> GetObjectsAsync<T>()
        {
            var objects = new List<object>();

            await using var stream = File.OpenRead(GetFullPath("TestData.json"));
            var reader = new StreamReader(stream);

            while (!reader.EndOfStream)
            {
                var line = await reader.ReadLineAsync();
                objects.Add(JObject.Parse(line!).ToObject<T>()!);
            }

            return objects.ToArray();
        }

        [Fact]
        public async void ProcessesObjectsSuccessfullyWithColumns()
        {
            await WithSqliteInMemoryConnection(async connection =>
            {
                await connection
                    .Add()
                    .FromObjects("TestData", await GetObjectsAsync<TestDataRecord>())
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
        public async void ProcessesObjectsSuccessfullyWithoutColumns()
        {
            await WithSqliteInMemoryConnection(async connection =>
            {
                await connection
                    .Add()
                    .FromObjects("TestData", await GetObjectsAsync<TestDataRecord>())
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
        public async void ProcessesObjectsSuccessfullyWithoutColumnsNested()
        {
            await WithSqliteInMemoryConnection(async connection =>
            {
                await connection
                    .Add()
                    .FromObjects("TestData", await GetObjectsAsync<TestDataRecordNested>())
                    .ExecuteAsync();

                var table = await connection.LoadTableAsync("TestData");
                table
                    .Should()
                    .Exist().And
                    .HaveAColumnCountOf(9).And
                    .HaveARowCountOf(1000);
            });
        }

        [Fact]
        public async void ProcessesObjectsSuccessfullyWithoutColumnsNoNested()
        {
            await WithSqliteInMemoryConnection(async connection =>
            {
                await connection
                    .Add()
                    .FromObjects("TestData", await GetObjectsAsync<TestDataRecordNested>())
                    .IgnoreNested()
                    .ExecuteAsync();

                var table = await connection.LoadTableAsync("TestData");
                table
                    .Should()
                    .Exist().And
                    .HaveAColumnCountOf(7).And
                    .HaveARowCountOf(1000);
            });
        }
    }
}
