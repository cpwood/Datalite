using System.IO;
using System.Reflection;
using System.Threading.Tasks;
using Azure.Data.Tables;
using Datalite.Testing;
using Newtonsoft.Json.Linq;

namespace Datalite.Sources.Databases.AzureTables.Tests.Integration
{
    public class AzuriteImage : DatabaseImage
    {
        /// <summary>
        /// Create an Azurite image with our test database configured.
        /// </summary>
        public AzuriteImage()
        {
            Build("mcr.microsoft.com/azure-storage/azurite", 10002);
        }

        /// <inheritdoc />
        public override int MaxAttempts => 10;

        /// <summary>
        /// Get the connection string for the Storage Account.
        /// </summary>
        /// <returns></returns>
        public string GetConnectionString()
        {
            return
                $"DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;TableEndpoint=http://{Address}:10002/devstoreaccount1;";
        }

        /// <inheritdoc />
        protected override async Task CheckConnectionAsync()
        {
            var serviceClient = new TableServiceClient(GetConnectionString());
            var pageable = serviceClient.QueryAsync();

            await foreach (var _ in pageable)
            {
            }
        }

        /// <inheritdoc />
        protected override async Task OnStartupAsync()
        {
            var tableClient = new TableClient(GetConnectionString(), "TestData");
            await tableClient.CreateIfNotExistsAsync();

            // TestData
            var dll = new FileInfo(Assembly.GetExecutingAssembly().Location);
            var file = Path.Combine(dll.DirectoryName!, "Integration", "TestData.json");

            using var reader = new StreamReader(file);
     
            while (!reader.EndOfStream)
            {
                var line = await reader.ReadLineAsync();

                if (string.IsNullOrEmpty(line)) continue;

                var obj = JObject.Parse(line);
                var item = new TableEntity(obj["last_name"]!.Value<string>(), obj["id"]!.Value<int>().ToString())
                {
                    ["id"] = obj["id"]!.Value<int>(),
                    ["first_name"] = obj["first_name"]!.Value<string>(),
                    ["last_name"] = obj["last_name"]!.Value<string>(),
                    ["gender"] = obj["gender"]!.Value<string>(),
                    ["image"] = obj["image"]!.Value<string>()
                };

                if (obj.ContainsKey("email"))
                    item["email"] = obj["email"]!.Value<string>();

                if (obj.ContainsKey("salary"))
                    item["salary"] = obj["salary"]!.Value<decimal>();

                await tableClient.AddEntityAsync(item);
            }

            // Foo
            tableClient = new TableClient(GetConnectionString(), "Foo");
            await tableClient.CreateIfNotExistsAsync();
            await tableClient.AddEntityAsync(new TableEntity("Foo", "Bar"));
        }
    }
}
