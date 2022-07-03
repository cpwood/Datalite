using System;
using System.Collections.Generic;
using System.Data;
using Datalite.Destination;
using Datalite.Sources;
using FluentAssertions;
using Moq;
using Moq.DataReader;
using Xunit;

namespace Datalite.Tests.Sources
{
    public class DataReaderExtensionsTests
    {
        private static string RunTest<T>(T value, StoragesClasses.StorageClassType type)
        {
            var data = new List<ReaderRecord<T>>() { new() { ColumnValue = value } };
            var mock = new Mock<IDataReader>();

            mock.SetupDataReader(data);

            var reader = mock.Object;
            reader.Read();

            return reader.GetSqlValue(new Column("ColumnValue", typeof(int), type, false));
        }

        [Fact]
        public void BoolSuccessful()
        {
            const bool value = true;
            RunTest(value, StoragesClasses.StorageClassType.IntegerClass).Should().Be("1");

            bool? value2 = null;
            RunTest(value2, StoragesClasses.StorageClassType.IntegerClass).Should().Be("NULL");

            value2 = value;
            RunTest(value2, StoragesClasses.StorageClassType.IntegerClass).Should().Be("1");
        }

        [Fact]
        public void ByteSuccessful()
        {
            const byte value = 1;
            RunTest(value, StoragesClasses.StorageClassType.BlobClass).Should().Be("x'01'");

            byte? value2 = null;
            RunTest(value2, StoragesClasses.StorageClassType.BlobClass).Should().Be("NULL");

            value2 = value;
            RunTest(value2, StoragesClasses.StorageClassType.BlobClass).Should().Be("x'01'");
        }

        [Fact]
        public void SbyteSuccessful()
        {
            const sbyte value = 1;
            RunTest(value, StoragesClasses.StorageClassType.BlobClass).Should().Be("x'01'");

            sbyte? value2 = null;
            RunTest(value2, StoragesClasses.StorageClassType.BlobClass).Should().Be("NULL");

            value2 = value;
            RunTest(value2, StoragesClasses.StorageClassType.BlobClass).Should().Be("x'01'");
        }

        [Fact]
        public void CharSuccessful()
        {
            const char value = 'c';
            RunTest(value, StoragesClasses.StorageClassType.TextClass).Should().Be("'c'");

            char? value2 = null;
            RunTest(value2, StoragesClasses.StorageClassType.TextClass).Should().Be("NULL");

            value2 = value;
            RunTest(value2, StoragesClasses.StorageClassType.TextClass).Should().Be("'c'");
        }

        [Fact]
        public void DecimalSuccessful()
        {
            const decimal value = 1.5M;
            RunTest(value, StoragesClasses.StorageClassType.NumericClass).Should().Be("1.5");

            decimal? value2 = null;
            RunTest(value2, StoragesClasses.StorageClassType.NumericClass).Should().Be("NULL");

            value2 = value;
            RunTest(value2, StoragesClasses.StorageClassType.NumericClass).Should().Be("1.5");
        }

        [Fact]
        public void DoubleSuccessful()
        {
            const double value = 1.5D;
            RunTest(value, StoragesClasses.StorageClassType.NumericClass).Should().Be("1.5");

            double? value2 = null;
            RunTest(value2, StoragesClasses.StorageClassType.NumericClass).Should().Be("NULL");

            value2 = value;
            RunTest(value2, StoragesClasses.StorageClassType.NumericClass).Should().Be("1.5");
        }

        [Fact]
        public void FloatSuccessful()
        {
            const float value = 1.5F;
            RunTest(value, StoragesClasses.StorageClassType.NumericClass).Should().Be("1.5");

            float? value2 = null;
            RunTest(value2, StoragesClasses.StorageClassType.NumericClass).Should().Be("NULL");

            value2 = value;
            RunTest(value2, StoragesClasses.StorageClassType.NumericClass).Should().Be("1.5");
        }

        [Fact]
        public void IntSuccessful()
        {
            const int value = 5;
            RunTest(value, StoragesClasses.StorageClassType.IntegerClass).Should().Be("5");

            int? value2 = null;
            RunTest(value2, StoragesClasses.StorageClassType.IntegerClass).Should().Be("NULL");

            value2 = value;
            RunTest(value2, StoragesClasses.StorageClassType.IntegerClass).Should().Be("5");
        }

        [Fact]
        public void UintSuccessful()
        {
            const uint value = 5;
            RunTest(value, StoragesClasses.StorageClassType.IntegerClass).Should().Be("5");

            uint? value2 = null;
            RunTest(value2, StoragesClasses.StorageClassType.IntegerClass).Should().Be("NULL");

            value2 = value;
            RunTest(value2, StoragesClasses.StorageClassType.IntegerClass).Should().Be("5");
        }

        [Fact]
        public void LongSuccessful()
        {
            const long value = 5;
            RunTest(value, StoragesClasses.StorageClassType.IntegerClass).Should().Be("5");

            long? value2 = null;
            RunTest(value2, StoragesClasses.StorageClassType.IntegerClass).Should().Be("NULL");

            value2 = value;
            RunTest(value2, StoragesClasses.StorageClassType.IntegerClass).Should().Be("5");
        }

        [Fact]
        public void UlongSuccessful()
        {
            const ulong value = 5;
            RunTest(value, StoragesClasses.StorageClassType.IntegerClass).Should().Be("5");

            ulong? value2 = null;
            RunTest(value2, StoragesClasses.StorageClassType.IntegerClass).Should().Be("NULL");

            value2 = value;
            RunTest(value2, StoragesClasses.StorageClassType.IntegerClass).Should().Be("5");
        }

        [Fact]
        public void ShortSuccessful()
        {
            const short value = 5;
            RunTest(value, StoragesClasses.StorageClassType.IntegerClass).Should().Be("5");

            short? value2 = null;
            RunTest(value2, StoragesClasses.StorageClassType.IntegerClass).Should().Be("NULL");

            value2 = value;
            RunTest(value2, StoragesClasses.StorageClassType.IntegerClass).Should().Be("5");
        }

        [Fact]
        public void UshortSuccessful()
        {
            const ushort value = 5;
            RunTest(value, StoragesClasses.StorageClassType.IntegerClass).Should().Be("5");

            ushort? value2 = null;
            RunTest(value2, StoragesClasses.StorageClassType.IntegerClass).Should().Be("NULL");

            value2 = value;
            RunTest(value2, StoragesClasses.StorageClassType.IntegerClass).Should().Be("5");
        }

        [Fact]
        public void StringSuccessful()
        {
            const string value = "foo";
            RunTest(value, StoragesClasses.StorageClassType.TextClass).Should().Be("'foo'");

            string? value2 = null;
            RunTest(value2, StoragesClasses.StorageClassType.TextClass).Should().Be("NULL");

            value2 = value;
            RunTest(value2, StoragesClasses.StorageClassType.TextClass).Should().Be("'foo'");
        }

        [Fact]
        public void DateTimeSuccessful()
        {
            var value = new DateTime(2022, 7, 2);
            RunTest(value, StoragesClasses.StorageClassType.TextClass).Should().Be("'2022-07-02T00:00:00.0000000'");

            DateTime? value2 = null;
            RunTest(value2, StoragesClasses.StorageClassType.TextClass).Should().Be("NULL");

            value2 = value;
            RunTest(value2, StoragesClasses.StorageClassType.TextClass).Should().Be("'2022-07-02T00:00:00.0000000'");
        }

        [Fact]
        public void GuidSuccessful()
        {
            // Mock IDataReader doesn't accept Guid :(
        }

        [Fact]
        public void ByteArraySuccessful()
        {
            // Mock IDataReader doesn't accept byte[] :(
        }

        [Fact]
        public void SbyteArraySuccessful()
        {
            // Mock IDataReader doesn't accept sbyte[] :(
        }

        [Fact]
        public void CharArraySuccessful()
        {
            // Mock IDataReader doesn't accept char[] :(
        }
    }

    public class ReaderRecord<T>
    {
        public T? ColumnValue { get; set; }
    }
}
