using System.Collections.Generic;
using System.Threading.Tasks;
using Datalite.Destination;
using FluentAssertions;
using Xunit;

namespace Datalite.Sources.Files.Json.Tests.Unit
{
    public class JsonDataliteContextTests
    {
        [Fact]
        public void TableNameChangeSuccessful()
        {
            var context =
                // ReSharper disable once UseObjectOrCollectionInitializer
                new JsonDataliteContext("Foo.json", "OutputTable", false, _ => Task.CompletedTask)
                {
                    TableDefinition = new TableDefinition("OutputTable")
                    {
                        Columns = new Dictionary<string, Column>()
                        {
                            { "Foo", new Column("Foo", typeof(string), true) }
                        }
                    }
                };

            context.OutputTable = "Changed";

            context.TableDefinition.Name.Should().Be("Changed");
            context.TableDefinition.Columns["Foo"].Required.Should().BeTrue();
        }
    }
}
