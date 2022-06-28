using System.IO.Abstractions;
using Datalite.Exceptions;
using Datalite.Sources.Databases.Shared;

namespace Datalite.Sources.Databases.H2
{
    /// <summary>
    /// H2 Extension Methods
    /// </summary>
    public static class H2Extensions
    {
        /// <summary>
        /// Copy data from an H2 database.
        /// </summary>
        /// <param name="adc"></param>
        /// <param name="connection">The connection information for the H2 database.</param>
        /// <returns></returns>
        /// <exception cref="DataliteException"></exception>
        public static DatabaseCommand FromH2(this AddDataCommand adc, H2Connection connection)
        {
            if (connection == null)
                throw new DataliteException("A valid H2Connection object must be provided.");

            var service = new H2Service(connection, adc.Connection, new FileSystem(), new ProcessRunner());
            var context = new DatabaseDataliteContext(false, ctx => service.ExecuteAsync(ctx));

            return new DatabaseCommand(context);
        }
    }
}