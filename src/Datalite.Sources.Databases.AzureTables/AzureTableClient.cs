using Azure;
using Azure.Data.Tables;
using Azure.Data.Tables.Models;

namespace Datalite.Sources.Databases.AzureTables
{
    /// <summary>
    /// Wrapper class for Table Storage access that facilitates unit testing.
    /// </summary>
    internal class AzureTableClient : IAzureTableClient
    {
        private readonly string _connectionString;

        public AzureTableClient(string connectionString)
        {
            _connectionString = connectionString;
        }

        public AsyncPageable<TableItem> QueryTablesAsync()
        {
            var serviceClient = new TableServiceClient(_connectionString);
            return serviceClient.QueryAsync();
        }

        public AsyncPageable<TableEntity> QueryRecordsAsync(string table, string? filter = null)
        {
            var client = new TableClient(
                _connectionString,
                table);
            return client.QueryAsync<TableEntity>(filter);
        }
    }
}
