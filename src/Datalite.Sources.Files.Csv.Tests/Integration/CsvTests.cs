using System.IO;
using System.Reflection;
using Datalite.Destination;
using Datalite.Testing;
using Xunit;

namespace Datalite.Sources.Files.Csv.Tests.Integration
{
    public class CsvTests : TestBaseClass
    {
        private static string GetFullPath(string filename)
        {
            var dll = new FileInfo(Assembly.GetExecutingAssembly().Location);
            return Path.Combine(dll.DirectoryName!, "Integration", filename);
        }

        [Fact]
        public async void ReadsCsvSuccessfullyWithColumnsAndNulls()
        {
            await WithSqliteInMemoryConnection(async connection =>
            {
                await connection
                    .Add()
                    .FromCsv(GetFullPath("TestData.csv"))
                    .WithOptions(x =>
                    {
                        x.HasHeaderRecord = true;
                    })
                    .WithColumns(
                        new Column("id", typeof(int), true),
                        new Column("first_name", typeof(string), true),
                        new Column("last_name", typeof(string), true),
                        new Column("email", typeof(string), false, StringValueInterpretation.LiteralNullIsNull),
                        new Column("gender", typeof(string), true),
                        new Column("image", typeof(byte[]), true, StringValueInterpretation.Hex),
                        new Column("salary", typeof(decimal), false, StringValueInterpretation.LiteralNullIsNull)
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
        public async void ReadsCsvSuccessfullyWithColumnsAndEmptyStringAndCurrency()
        {
            await WithSqliteInMemoryConnection(async connection =>
            {
                await connection
                    .Add()
                    .FromCsv(GetFullPath("TestData2.csv"), "TestData")
                    .WithOptions(x =>
                    {
                        x.HasHeaderRecord = true;
                    })
                    .WithColumns(
                        new Column("id", typeof(int), true),
                        new Column("first_name", typeof(string), true),
                        new Column("last_name", typeof(string), true),
                        new Column("email", typeof(string), false, StringValueInterpretation.EmptyStringIsNull),
                        new Column("gender", typeof(string), true),
                        new Column("image", typeof(byte[]), true, StringValueInterpretation.Hex),
                        new Column("salary", typeof(decimal), false,
                            StringValueInterpretation.EmptyStringIsNull | 
                            StringValueInterpretation.StripAlpha)
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
        public async void ReadsCsvSuccessfullyWithoutColumns()
        {
            await WithSqliteInMemoryConnection(async connection =>
            {
                await connection
                    .Add()
                    .FromCsv(GetFullPath("TestData.csv"))
                    .WithOptions(x =>
                    {
                        x.HasHeaderRecord = true;
                    })
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
