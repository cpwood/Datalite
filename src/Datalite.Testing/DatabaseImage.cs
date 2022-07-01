using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Ductus.FluentDocker.Builders;
using Ductus.FluentDocker.Executors;
using Ductus.FluentDocker.Executors.Parsers;
using Ductus.FluentDocker.Extensions;
using Ductus.FluentDocker.Services;
using Ductus.FluentDocker.Services.Extensions;

namespace Datalite.Testing
{
    /// <summary>
    /// Orchestrates the running of Docker images for running tests against database servers.
    /// </summary>
    public abstract class DatabaseImage : IDisposable
    {
        /// <summary>
        /// The Docker container.
        /// </summary>
        protected IContainerService? Container;

        /// <summary>
        /// The IP address used by the running database image.
        /// </summary>
        protected string Address { get; private set; } = string.Empty;

        /// <summary>
        /// Number of attempts to make before giving up on getting a successful connection.
        /// </summary>
        public abstract int MaxAttempts { get; }

        /// <summary>
        /// Build the database image and run it. This method needs to be synchronous since it will
        /// be called from a subclass' constructor.
        /// </summary>
        /// <param name="image">The image name.</param>
        /// <param name="environmentVariables">Any environment variables.</param>
        /// <param name="port">The port number to expose.</param>
        public void Build(string image, int port, string[]? environmentVariables = null)
        {
            Build(image, new[] { port }, environmentVariables);
        }

        /// <summary>
        /// Build the database image and run it. This method needs to be synchronous since it will
        /// be called from a subclass' constructor.
        /// </summary>
        /// <param name="image">The image name.</param>
        /// <param name="environmentVariables">Any environment variables.</param>
        /// <param name="ports">The port numbers to expose.</param>
        public void Build(string image, int[] ports, string[]? environmentVariables = null)
        {
            Console.WriteLine("Starting container..");

            var builder = new Builder().UseContainer()
                .UseImage(image);

            if (environmentVariables != null && environmentVariables.Any())
                builder = builder.WithEnvironment(environmentVariables);

            foreach (var port in ports)
            {
                builder = builder
                    .ExposePort(port, port)
                    .WaitForPort($"{port}/tcp", 30000 /*30s*/);
            }

            Container = builder.Build();

            try
            {
                Container.Start();

                Console.Write("Started. Getting connection details.. ");

                // Get the IP address and port number to use for connections.
                var ep = Container.ToHostExposedEndpoint($"{ports.First()}/tcp");
                Address = ep.Address.ToString();

                if (Address == "0.0.0.0")
                    Address = "127.0.0.1";

                foreach (var port in ports)
                {
                    Console.WriteLine($"{Address}:{port}");
                }

                Task.Run(async () =>
                {
                    Console.WriteLine("Wait for a usable connection before continuing..");

                    for (var i = 0; i < MaxAttempts; i++)
                    {
                        try
                        {
                            await CheckConnectionAsync();
                            break;
                        }
                        catch (Exception)
                        {
                            if (i == MaxAttempts - 1)
                                throw;

                            await Task.Delay(1000);
                        }
                    }

                    Console.WriteLine("Perform startup actions..");
                    await OnStartupAsync();
                }).Wait();

                Console.WriteLine("Container ready!");
            }
            catch
            {
                Container.Dispose();
                throw;
            }
        }

        /// <summary>
        /// Check that connection is usable before running startup tasks.
        /// </summary>
        /// <returns>True if a usable connection has been found.</returns>
        protected abstract Task CheckConnectionAsync();

        /// <summary>
        /// Perform any startup tasks, such as data scaffolding.
        /// </summary>
        /// <returns></returns>
        protected abstract Task OnStartupAsync();

        /// <summary>
        /// Stop the running Docker container.
        /// </summary>
        public void Dispose()
        {
            if (Container == null)
                return;

            try
            {
                var c = Container;
                Container = null;

                var image = c.Image.Id;

                c.Dispose();

                // This environment variable will only be set as part of the GitHub Action.
                // Images will be retained in development.
                if (bool.Parse(Environment.GetEnvironmentVariable("REMOVE_IMAGES") ?? "false"))
                {
                    // Remove the image from the host to conserve disk space.
                    new ProcessExecutor<StringListResponseParser, IList<string>>(
                            "docker".ResolveBinary(),
                            $"rmi {image}").Execute();
                }
            }
            catch
            {
                // Ignore
            }
        }
    }
}