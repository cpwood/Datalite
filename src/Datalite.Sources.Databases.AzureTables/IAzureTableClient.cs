using Azure;
using Azure.Data.Tables;
using Azure.Data.Tables.Models;

namespace Datalite.Sources.Databases.AzureTables
{
    /// <summary>
    /// Wrapper interface for Table Storage access that facilitates unit testing.
    /// </summary>
    public interface IAzureTableClient
    {
        AsyncPageable<TableItem> QueryTablesAsync();
        AsyncPageable<TableEntity> QueryRecordsAsync(string table, string? filter = null);
    }
}
