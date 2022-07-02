using System.IO;
using System.IO.Abstractions;
using System.Reflection;
using System.Threading.Tasks;
using Datalite.Destination;
using Datalite.Exceptions;
using Datalite.Testing;
using FluentAssertions;
using Xunit;

namespace Datalite.Sources.Files.Json.Tests.Unit
{
    public class JsonExtensionsTests : TestBaseClass
    {
        private static string GetFullPath()
        {
            var dll = new FileInfo(Assembly.GetExecutingAssembly().Location);
            return Path.Combine(dll.DirectoryName!, "Integration", "TestData.json");
        }

        [Fact]
        public async void FileAccepted()
        {
            await WithSqliteInMemoryConnection(conn =>
            {
                var builder = conn.Add().FromJson(GetFullPath());
                builder.Should().NotBeNull();
                return Task.CompletedTask;
            });
        }

        [Fact]
        public async void NullFilenameRejected()
        {
            await WithSqliteInMemoryConnection(conn =>
            {
                conn
                    .Add()
                    .Invoking(x => x.FromJson(string.Empty))
                    .Should()
                    .Throw<DataliteException>()
                    .WithMessage("The path to a JSON file must be provided.");

                return Task.CompletedTask;
            });
        }

        [Fact]
        public async void NoSuchFileRejected()
        {
            await WithSqliteInMemoryConnection(conn =>
            {
                conn
                    .Add()
                    .Invoking(x => x.FromJson("DoesNotExist.json"))
                    .Should()
                    .Throw<DataliteException>()
                    .WithMessage("The JSON file does not exist!");

                return Task.CompletedTask;
            });
        }

        [Fact]
        public async void FileAndTableAccepted()
        {
            await WithSqliteInMemoryConnection(conn =>
            {
                var builder = conn.Add().FromJson(GetFullPath(), "OutputTable");
                builder.Should().NotBeNull();
                return Task.CompletedTask;
            });
        }

        [Fact]
        public async void FileAndTableAndFilesystemAccepted()
        {
            await WithSqliteInMemoryConnection(conn =>
            {
                var builder = conn.Add().FromJson(GetFullPath(), "OutputTable", true, new FileSystem());
                builder.Should().NotBeNull();
                return Task.CompletedTask;
            });
        }

        [Fact]
        public async void NullFilenameAndTableAndFilesystemRejected()
        {
            await WithSqliteInMemoryConnection(conn =>
            {
                conn
                    .Add()
                    .Invoking(x => x.FromJson(string.Empty, "OutputTable", true, new FileSystem()))
                    .Should()
                    .Throw<DataliteException>()
                    .WithMessage("The path to a JSON file must be provided.");

                return Task.CompletedTask;
            });
        }

        [Fact]
        public async void NoSuchFileAndTableAndFilesystemRejected()
        {
            await WithSqliteInMemoryConnection(conn =>
            {
                conn
                    .Add()
                    .Invoking(x => x.FromJson("DoesNotExist.json", "OutputTable", true, new FileSystem()))
                    .Should()
                    .Throw<DataliteException>()
                    .WithMessage("The JSON file does not exist!");

                return Task.CompletedTask;
            });
        }

        [Fact]
        public async void FilenameAndInvalidTableAndFilesystemRejected()
        {
            await WithSqliteInMemoryConnection(conn =>
            {
                conn
                    .Add()
                    .Invoking(x => x.FromJson(GetFullPath(), string.Empty, true, new FileSystem()))
                    .Should()
                    .Throw<DataliteException>()
                    .WithMessage("A valid output table name must be provided.");

                return Task.CompletedTask;
            });
        }
    }
}
