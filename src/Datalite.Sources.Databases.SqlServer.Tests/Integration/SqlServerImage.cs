using System.Data.SqlClient;
using System.IO;
using System.Reflection;
using System.Threading.Tasks;
using Datalite.Testing;

namespace Datalite.Sources.Databases.SqlServer.Tests.Integration
{
    public class SqlServerImage : DatabaseImage
    {
        /// <summary>
        /// Create a SQL Server image with our test database configured.
        /// </summary>
        public SqlServerImage()
        {
            Build("mcr.microsoft.com/mssql/server:2022-latest",
                1433,
                new[] { "ACCEPT_EULA=Y", "SA_PASSWORD=p455w0rd!2" });
        }

        /// <inheritdoc />
        public override int MaxAttempts => 50;

        /// <summary>
        /// Build a connection string for the specified database name.
        /// </summary>
        /// <param name="database">The database name.</param>
        /// <returns>A connection string.</returns>
        public string GetConnectionString(string database)
        {
            return $"Persist Security Info=False;User ID=sa;Password=p455w0rd!2;Initial Catalog={database};Server={Address},1433";
        }

        /// <inheritdoc />
        protected override async Task CheckConnectionAsync()
        {
            await using var conn = new SqlConnection(GetConnectionString("master"));
            await conn.OpenAsync();
            await conn.CloseAsync();
        }

        /// <inheritdoc />
        protected override async Task OnStartupAsync()
        {
            var dll = new FileInfo(Assembly.GetExecutingAssembly().Location);
            var file = Path.Combine(dll.DirectoryName!, "Integration", "TestData.sql");
            var script = await File.ReadAllTextAsync(file);

            await using var sqlConn = new SqlConnection(GetConnectionString("master"));
            await sqlConn.OpenAsync();

            var scaffolder = new DataScaffolder(sqlConn);
            await scaffolder.ScaffoldAsync(script);
        }
    }
}
