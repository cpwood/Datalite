﻿using Datalite.Exceptions;
using Datalite.Sources.Databases.Shared;
using FluentAssertions;
using Xunit;

namespace Datalite.Sources.Databases.SqlServer.Tests.Unit
{
    public class SqlServerServiceTests
    {
        [Fact]
        public void SchemaNameRejected()
        {
            var service = new SqlServerService(null!, null!);

            service
                .Invoking(x => x.ValidateTableIdentifier(new TableIdentifier("0", "Foo")))
                .Should()
                .Throw<DataliteException>()
                .WithMessage("The provided schema name*");
        }

        [Fact]
        public void TableNameRejected()
        {
            var service = new SqlServerService(null!, null!);

            service
                .Invoking(x => x.ValidateTableIdentifier(new TableIdentifier("Foo", "0")))
                .Should()
                .Throw<DataliteException>()
                .WithMessage("The provided table name*");
        }
    }
}
