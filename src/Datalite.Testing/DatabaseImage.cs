using System;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using Ductus.FluentDocker.Builders;
using Ductus.FluentDocker.Services;
using Ductus.FluentDocker.Services.Extensions;

namespace Datalite.Testing
{
    public abstract class DatabaseImage<T> where T : DbConnection, IDisposable, new()
    {
        protected IContainerService? Container;

        /// <summary>
        /// The IP address used by the running database image.
        /// </summary>
        protected string Address { get; private set; } = string.Empty;

        /// <summary>
        /// The port number on the host system that's mapped to the image's database port.
        /// </summary>
        protected int Port { get; private set; }

        /// <summary>
        /// Number of attempts to make before giving up on getting a successful connection.
        /// </summary>
        public abstract int MaxAttempts { get; }

        /// <summary>
        /// The name of the out-of-the-box database that will already exist upon startup. E.g.,
        /// for SQL Server, this is "master". A connection to this database will be used to
        /// check that the server is fully started and ready for commands.
        /// </summary>
        public abstract string DefaultDatabase { get; }

        /// <summary>
        /// Build a connection string for the specified database name.
        /// </summary>
        /// <param name="database">The database name.</param>
        /// <returns>A connection string.</returns>
        public abstract string GetConnectionString(string database);

        /// <summary>
        /// Build the database image and run it. This method needs to be synchronous since it will
        /// be called from a subclass' constructor.
        /// </summary>
        /// <param name="image">The image name.</param>
        /// <param name="environmentVariables">Any environment variables.</param>
        /// <param name="port">The port number to expose.</param>
        public void Build(string image, string[] environmentVariables, int port)
        {
            Console.WriteLine("Starting container..");

            Container = new Builder().UseContainer()
                .UseImage(image)
                .WithEnvironment(environmentVariables)
                .ExposePort(port)
                .WaitForPort($"{port}/tcp", 30000 /*30s*/)
                .Build();

            try
            {
                Container.Start();

                Console.WriteLine("Started. Getting connection details..");

                // Get the IP address and port number to use for connections.
                var ep = Container.ToHostExposedEndpoint($"{port}/tcp");
                Address = ep.Address.ToString();
                Port = ep.Port;

                if (Address == "0.0.0.0")
                    Address = "127.0.0.1";

                Console.WriteLine($"{Address}:{Port}");

                Console.WriteLine("Wait for a usable connection before continuing..");

                using var conn = new T();
                conn.ConnectionString = GetConnectionString(DefaultDatabase);

                for (var i = 0; i < MaxAttempts; i++)
                {
                    try
                    {
                        conn.Open();
                        break;
                    }
                    catch (Exception)
                    {
                        if (i == MaxAttempts - 1)
                            throw;

                        Thread.Sleep(1000);
                    }
                }

                conn.Close();

                // Perform any startup scaffolding, etc.
                Console.WriteLine("Perform startup actions..");

                Task.Run(async () => await OnStartupAsync()).Wait();

                Console.WriteLine("Done!");
            }
            catch
            {
                Container.Dispose();
                throw;
            }
        }

        /// <summary>
        /// Perform any startup tasks, such as data scaffolding.
        /// </summary>
        /// <returns></returns>
        protected abstract Task OnStartupAsync();

        public void Dispose()
        {
            try
            {
                var c = Container;
                Container = null;
                c?.Dispose();
            }
            catch
            {
                // Ignore
            }
        }
    }
}