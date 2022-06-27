using System;
using System.Data.SqlClient;
using System.IO;
using System.Reflection;
using System.Threading.Tasks;
using Datalite.Testing;

namespace Datalite.Sources.Databases.SqlServer.Tests
{
    public class SqlServerImage : DatabaseImage<SqlConnection>, IDisposable
    {
        /// <summary>
        /// Create a SQL Server image with our test database configured.
        /// </summary>
        public SqlServerImage()
        {
            Build("mcr.microsoft.com/mssql/server:2022-latest",
                new[] { "ACCEPT_EULA=Y", "SA_PASSWORD=p455w0rd!2" },
                1433);
        }

        /// <inheritdoc />
        public override int MaxAttempts => 50;

        /// <inheritdoc />
        public override string DefaultDatabase => "master";

        /// <inheritdoc />
        public override string GetConnectionString(string database)
        {
            return $"Persist Security Info=False;User ID=sa;Password=p455w0rd!2;Initial Catalog={database};Server={Address},{Port}";
        }

        /// <inheritdoc />
        protected override async Task OnStartupAsync()
        {
            var dll = new FileInfo(Assembly.GetExecutingAssembly().Location);
            var file = Path.Combine(dll.DirectoryName!, "TestData.sql");
            var script = await File.ReadAllTextAsync(file);

            await using var sqlConn = new SqlConnection(GetConnectionString("master"));
            await sqlConn.OpenAsync();

            var scaffolder = new DataScaffolder(sqlConn);
            await scaffolder.ScaffoldAsync(script);
        }
    }
}
