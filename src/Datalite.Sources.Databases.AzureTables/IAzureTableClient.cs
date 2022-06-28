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
        /// <summary>
        /// Gets a list of tables.
        /// </summary>
        /// <returns>A pageable list of tables.</returns>
        AsyncPageable<TableItem> QueryTablesAsync();

        /// <summary>
        /// Retrieves rows from a table using an optional filter.
        /// </summary>
        /// <param name="table">The table.</param>
        /// <param name="filter">An optional filter.  The string syntax is described
        /// at <see href="https://docs.microsoft.com/en-us/visualstudio/azure/vs-azure-tools-table-designer-construct-filter-strings?view=vs-2022">this page</see>.</param>
        /// <returns></returns>
        AsyncPageable<TableEntity> QueryRecordsAsync(string table, string? filter = null);
    }
}
