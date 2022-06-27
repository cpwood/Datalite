using Datalite.Exceptions;

namespace Datalite.Sources.Databases.AzureTables
{
    public static class AzureTableExtensions
    {
        /// <summary>
        /// Load data from one or more Azure Storage Tables.
        /// </summary>
        /// <param name="adc"></param>
        /// <param name="connectionString">The connection string for the Azure Storage account.</param>
        /// <returns></returns>
        /// <exception cref="DataliteException"></exception>
        public static AzureTablesCommand FromAzureTables(this AddDataCommand adc, string connectionString)
        {
            if (string.IsNullOrEmpty(connectionString))
                throw new DataliteException("An Azure Table Storage connection string must be provided.");

            var service = new AzureTableService(adc.Connection, new AzureTableClient(connectionString));
            var context = new AzureTablesDataliteContext(ctx => service.ExecuteAsync(ctx));

            return new AzureTablesCommand(context);
        }
    }
}