using System.IO;
using System.Net.Http;
using System.Reflection;
using System.Threading.Tasks;
using Datalite.Testing;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json.Linq;

namespace Datalite.Sources.Databases.CosmosDb.Tests.Integration
{
    public class CosmosDbImage : DatabaseImage
    {
        /// <summary>
        /// Create an Azurite image with our test database configured.
        /// </summary>
        public CosmosDbImage()
        {
            CosmosDbClient.CosmosOptions = new CosmosClientOptions
            {
                // Things can get a bit slow on low-powered machines!
                //RequestTimeout = TimeSpan.FromMinutes(1),

                // Speed up the insert of data.
                //AllowBulkExecution = true,

                // Turn off validation of HTTPS certificates.
                HttpClientFactory = () =>
                {
                    HttpMessageHandler httpMessageHandler = new HttpClientHandler()
                    {
                        ServerCertificateCustomValidationCallback =
                            HttpClientHandler.DangerousAcceptAnyServerCertificateValidator
                    };

                    return new HttpClient(httpMessageHandler);
                },
                ConnectionMode = ConnectionMode.Gateway
            };

            Build("mcr.microsoft.com/cosmosdb/linux/azure-cosmos-emulator",
                new[] { 8081, 10251, 10252, 10253, 10254 },
                new[]
                {
                    "AZURE_COSMOS_EMULATOR_PARTITION_COUNT=20",
                    "AZURE_COSMOS_EMULATOR_ENABLE_DATA_PERSISTENCE=true",
                    "AZURE_COSMOS_EMULATOR_IP_ADDRESS_OVERRIDE=127.0.0.1"
                }
            );
        }

        /// <inheritdoc />
        public override int MaxAttempts => 10;

        public static string Url => "https://localhost:8081";
        public static string Key => "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==";

        /// <inheritdoc />
        protected override async Task CheckConnectionAsync()
        {
            var client = new CosmosClient(Url, Key, CosmosDbClient.CosmosOptions);
            await client.ReadAccountAsync();
        }

        /// <inheritdoc />
        protected override async Task OnStartupAsync()
        {
            var client = new CosmosClient(Url, Key, CosmosDbClient.CosmosOptions);

            var databaseResponse =
                await client.CreateDatabaseAsync("UnitTests", 10000);

            await databaseResponse.Database.CreateContainerAsync("MyData", "/gender");
            
            var container = client.GetContainer("UnitTests", "MyData");
            
            // TestData
            var dll = new FileInfo(Assembly.GetExecutingAssembly().Location);
            var file = Path.Combine(dll.DirectoryName!, "Integration", "TestData.json");

            using var reader = new StreamReader(file);
       
            while (!reader.EndOfStream)
            {
                var line = await reader.ReadLineAsync();

                if (string.IsNullOrEmpty(line)) continue;

                var obj = JObject.Parse(line);
                obj["id"] = obj["id"]!.Value<int>().ToString();

                await container.CreateItemAsync(obj, new PartitionKey(obj["gender"]!.Value<string>()));
            }
        }
    }
}
