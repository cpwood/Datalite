using System.IO;
using System.Reflection;
using System.Threading.Tasks;
using Datalite.Testing;
using Npgsql;

namespace Datalite.Sources.Databases.Postgres.Tests.Integration
{
    public class PostgresImage : DatabaseImage
    {
        /// <summary>
        /// Create a SQL Server image with our test database configured.
        /// </summary>
        public PostgresImage()
        {
            Build("postgres:latest",
                5432,
                new[] { "POSTGRES_PASSWORD=mysecretpassword" });
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
            return $"Host={Address};Username=postgres;Password=mysecretpassword;Database={database}";
        }

        /// <inheritdoc />
        protected override async Task CheckConnectionAsync()
        {
            await using var conn = new NpgsqlConnection(GetConnectionString("postgres"));
            await conn.OpenAsync();
            await conn.CloseAsync();
        }

        /// <inheritdoc />
        protected override async Task OnStartupAsync()
        {
            var dll = new FileInfo(Assembly.GetExecutingAssembly().Location);
            var file = Path.Combine(dll.DirectoryName!, "Integration", "TestData.sql");
            var script = await File.ReadAllTextAsync(file);

            await using (var sqlConn = new NpgsqlConnection(GetConnectionString("postgres")))
            {
                var cmd = new NpgsqlCommand("create database \"Datalite\";", sqlConn);
                await sqlConn.OpenAsync();
                await cmd.ExecuteNonQueryAsync();
                await sqlConn.CloseAsync();
            }

            await using (var sqlConn = new NpgsqlConnection(GetConnectionString("Datalite")))
            {
                await sqlConn.OpenAsync();

                var scaffolder = new DataScaffolder(sqlConn);
                await scaffolder.ScaffoldAsync(script);
            }
        }
    }
}
