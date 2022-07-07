using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.IO;
using System.Linq;
using Azure;
using Azure.Core;
using Azure.Data.Tables;
using Azure.Data.Tables.Models;

namespace Datalite.Sources.Databases.AzureTables.Tests
{
    internal class FakeAzureTableClient : IAzureTableClient
    {
        public string[]? Tables { get; set; }
        public TableEntity[]? Records { get; set; }

        public AsyncPageable<TableItem> QueryTablesAsync()
        {
            if (Tables == null)
                throw new NullReferenceException("Tables haven't been initialised");

            var tables = new ReadOnlyCollection<TableItem>(Tables.Select(x => new TableItem(x)).ToList());
            var pages = new List<Page<TableItem>>();
            var page = Page<TableItem>.FromValues(tables, null, new FakeResponse());
            pages.Add(page);
            return AsyncPageable<TableItem>.FromPages(pages);
        }

        public AsyncPageable<TableEntity> QueryRecordsAsync(string table, string? filter = null)
        {
            if (Records == null)
                throw new NullReferenceException("Records haven't been initialised");

            var records = new ReadOnlyCollection<TableEntity>(Records.ToList());
            var pages = new List<Page<TableEntity>>();
            var page = Page<TableEntity>.FromValues(records, null, new FakeResponse());
            pages.Add(page);
            return AsyncPageable<TableEntity>.FromPages(pages);
        }
    }

    internal class FakeResponse : Response
    {
        public override void Dispose()
        {
        }

        protected override bool TryGetHeader(string name, out string value)
        {
            value = string.Empty;
            return false;
        }

        protected override bool TryGetHeaderValues(string name, out IEnumerable<string> values)
        {
            values = Array.Empty<string>();
            return false;
        }

        protected override bool ContainsHeader(string name)
        {
            return false;
        }

        protected override IEnumerable<HttpHeader> EnumerateHeaders()
        {
            return Array.Empty<HttpHeader>();
        }

        public override int Status => 200;
        public override string ReasonPhrase => string.Empty;
        public override Stream? ContentStream { get; set; }
        public override string ClientRequestId { get; set; } = Guid.NewGuid().ToString();
    }
}
