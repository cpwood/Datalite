using System.IO;
using System.IO.Abstractions;
using System.Reflection;
using System.Threading.Tasks;
using Datalite.Destination;
using Datalite.Exceptions;
using Datalite.Testing;
using FluentAssertions;
using Xunit;

namespace Datalite.Sources.Files.Parquet.Tests.Unit
{
    public class ParquetExtensionsTests : TestBaseClass
    {
        private static string GetFullPath()
        {
            var dll = new FileInfo(Assembly.GetExecutingAssembly().Location);
            return Path.Combine(dll.DirectoryName!, "Integration", "TestData.parquet");
        }

        [Fact]
        public async void FileAccepted()
        {
            await WithSqliteInMemoryConnection(conn =>
            {
                var builder = conn.Add().FromParquet(GetFullPath());
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
                    .Invoking(x => x.FromParquet(string.Empty))
                    .Should()
                    .Throw<DataliteException>()
                    .WithMessage("The path to a Parquet file must be provided.");

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
                    .Invoking(x => x.FromParquet("DoesNotExist.parquet"))
                    .Should()
                    .Throw<DataliteException>()
                    .WithMessage("The Parquet file does not exist!");

                return Task.CompletedTask;
            });
        }

        [Fact]
        public async void FileAndTableAccepted()
        {
            await WithSqliteInMemoryConnection(conn =>
            {
                var builder = conn.Add().FromParquet(GetFullPath(), "OutputTable");
                builder.Should().NotBeNull();
                return Task.CompletedTask;
            });
        }

        [Fact]
        public async void FileAndTableAndFilesystemAccepted()
        {
            await WithSqliteInMemoryConnection(conn =>
            {
                var builder = conn.Add().FromParquet(GetFullPath(), "OutputTable", new FileSystem());
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
                    .Invoking(x => x.FromParquet(string.Empty, "OutputTable", new FileSystem()))
                    .Should()
                    .Throw<DataliteException>()
                    .WithMessage("The path to a Parquet file must be provided.");

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
                    .Invoking(x => x.FromParquet("DoesNotExist.parquet", "OutputTable", new FileSystem()))
                    .Should()
                    .Throw<DataliteException>()
                    .WithMessage("The Parquet file does not exist!");

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
                    .Invoking(x => x.FromParquet(GetFullPath(), string.Empty, new FileSystem()))
                    .Should()
                    .Throw<DataliteException>()
                    .WithMessage("An output table name must be provided.");

                return Task.CompletedTask;
            });
        }
    }
}
