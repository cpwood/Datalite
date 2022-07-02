using System;
using System.Data.SqlClient;
using Datalite.Destination;
using FluentAssertions;
using Newtonsoft.Json.Linq;
using Xunit;

namespace Datalite.Tests.Destination
{
    public class StorageClassesTests
    {
        [Fact]
        public void AllTypesMappedSuccessfully()
        {
            StoragesClasses.FromType(typeof(bool)).Should().Be(StoragesClasses.StorageClassType.IntegerClass);
            StoragesClasses.FromType(typeof(byte)).Should().Be(StoragesClasses.StorageClassType.BlobClass);
            StoragesClasses.FromType(typeof(byte[])).Should().Be(StoragesClasses.StorageClassType.BlobClass);
            StoragesClasses.FromType(typeof(sbyte)).Should().Be(StoragesClasses.StorageClassType.BlobClass);
            StoragesClasses.FromType(typeof(sbyte[])).Should().Be(StoragesClasses.StorageClassType.BlobClass);
            StoragesClasses.FromType(typeof(char)).Should().Be(StoragesClasses.StorageClassType.TextClass);
            StoragesClasses.FromType(typeof(char[])).Should().Be(StoragesClasses.StorageClassType.TextClass);
            StoragesClasses.FromType(typeof(decimal)).Should().Be(StoragesClasses.StorageClassType.NumericClass);
            StoragesClasses.FromType(typeof(double)).Should().Be(StoragesClasses.StorageClassType.RealClass);
            StoragesClasses.FromType(typeof(float)).Should().Be(StoragesClasses.StorageClassType.RealClass);
            StoragesClasses.FromType(typeof(int)).Should().Be(StoragesClasses.StorageClassType.IntegerClass);
            StoragesClasses.FromType(typeof(uint)).Should().Be(StoragesClasses.StorageClassType.IntegerClass);
            StoragesClasses.FromType(typeof(long)).Should().Be(StoragesClasses.StorageClassType.IntegerClass);
            StoragesClasses.FromType(typeof(ulong)).Should().Be(StoragesClasses.StorageClassType.IntegerClass);
            StoragesClasses.FromType(typeof(short)).Should().Be(StoragesClasses.StorageClassType.IntegerClass);
            StoragesClasses.FromType(typeof(ushort)).Should().Be(StoragesClasses.StorageClassType.IntegerClass);
            StoragesClasses.FromType(typeof(string)).Should().Be(StoragesClasses.StorageClassType.TextClass);
            StoragesClasses.FromType(typeof(DateTime)).Should().Be(StoragesClasses.StorageClassType.TextClass);
            StoragesClasses.FromType(typeof(DateTimeOffset)).Should().Be(StoragesClasses.StorageClassType.TextClass);
            StoragesClasses.FromType(typeof(Guid)).Should().Be(StoragesClasses.StorageClassType.TextClass);
            StoragesClasses.FromType(typeof(JObject)).Should().Be(StoragesClasses.StorageClassType.TextClass);
            StoragesClasses.FromType(typeof(JArray)).Should().Be(StoragesClasses.StorageClassType.TextClass);
            StoragesClasses.FromType(typeof(DBNull)).Should().Be(StoragesClasses.StorageClassType.IntegerClass);
            StoragesClasses.FromType(typeof(UnknownDataType)).Should().Be(StoragesClasses.StorageClassType.IntegerClass);

            var action = () => StoragesClasses.FromType(typeof(SqlConnection));
            action.Should().Throw<ArgumentOutOfRangeException>();
        }
    }
}
