using System.IO;
using System.IO.Abstractions;
using System.Reflection;
using System.Threading.Tasks;
using Datalite.Destination;
using Datalite.Exceptions;
using Datalite.Testing;
using FluentAssertions;
using Xunit;

namespace Datalite.Sources.Files.Csv.Tests.Unit
{
    public class CsvExtensionsTests : TestBaseClass
    {
        private static string GetFullPath()
        {
            var dll = new FileInfo(Assembly.GetExecutingAssembly().Location);
            return Path.Combine(dll.DirectoryName!, "Integration", "TestData.csv");
        }

        [Fact]
        public async Task FileAccepted()
        {
            await WithSqliteInMemoryConnection(conn =>
            {
                var builder = conn.Add().FromCsv(GetFullPath());
                builder.Should().NotBeNull();
                return Task.CompletedTask;
            });
        }

        [Fact]
        public async Task NullFilenameRejected()
        {
            await WithSqliteInMemoryConnection(conn =>
            {
                conn
                    .Add()
                    .Invoking(x => x.FromCsv(string.Empty))
                    .Should()
                    .Throw<DataliteException>()
                    .WithMessage("The path to a CSV file must be provided.");

                return Task.CompletedTask;
            });
        }

        [Fact]
        public async Task NoSuchFileRejected()
        {
            await WithSqliteInMemoryConnection(conn =>
            {
                conn
                    .Add()
                    .Invoking(x => x.FromCsv("DoesNotExist.csv"))
                    .Should()
                    .Throw<DataliteException>()
                    .WithMessage("The CSV file does not exist!");

                return Task.CompletedTask;
            });
        }

        [Fact]
        public async Task FileAndTableAccepted()
        {
            await WithSqliteInMemoryConnection(conn =>
            {
                var builder = conn.Add().FromCsv(GetFullPath(), "OutputTable");
                builder.Should().NotBeNull();
                return Task.CompletedTask;
            });
        }

        [Fact]
        public async Task FileAndTableAndFilesystemAccepted()
        {
            await WithSqliteInMemoryConnection(conn =>
            {
                var builder = conn.Add().FromCsv(GetFullPath(), "OutputTable", new FileSystem());
                builder.Should().NotBeNull();
                return Task.CompletedTask;
            });
        }

        [Fact]
        public async Task NullFilenameAndTableAndFilesystemRejected()
        {
            await WithSqliteInMemoryConnection(conn =>
            {
                conn
                    .Add()
                    .Invoking(x => x.FromCsv(string.Empty, "OutputTable", new FileSystem()))
                    .Should()
                    .Throw<DataliteException>()
                    .WithMessage("The path to a CSV file must be provided.");

                return Task.CompletedTask;
            });
        }

        [Fact]
        public async Task NoSuchFileAndTableAndFilesystemRejected()
        {
            await WithSqliteInMemoryConnection(conn =>
            {
                conn
                    .Add()
                    .Invoking(x => x.FromCsv("DoesNotExist.csv", "OutputTable", new FileSystem()))
                    .Should()
                    .Throw<DataliteException>()
                    .WithMessage("The CSV file does not exist!");

                return Task.CompletedTask;
            });
        }

        [Fact]
        public async Task FilenameAndInvalidTableAndFilesystemRejected()
        {
            await WithSqliteInMemoryConnection(conn =>
            {
                conn
                    .Add()
                    .Invoking(x => x.FromCsv(GetFullPath(), string.Empty, new FileSystem()))
                    .Should()
                    .Throw<DataliteException>()
                    .WithMessage("An output table name must be provided.");

                return Task.CompletedTask;
            });
        }
    }
}
