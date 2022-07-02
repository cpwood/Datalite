using System;
using System.Globalization;
using Datalite.Destination;
using Datalite.Sources;
using FluentAssertions;
using Microsoft.Data.Sqlite;
using Newtonsoft.Json.Linq;
using Xunit;

namespace Datalite.Tests.Destination
{
    public class TypeExtensionsTests
    {
        [Fact]
        public void NullConversionCorrect()
        {
            const object? value = null;
            value.Convert(StoragesClasses.StorageClassType.IntegerClass).Should().Be("NULL");
            DBNull.Value.Convert(StoragesClasses.StorageClassType.IntegerClass).Should().Be("NULL");
        }

        [Fact]
        public void BoolConversionCorrect()
        {
            // Conversions
            true.Convert(StoragesClasses.StorageClassType.IntegerClass).Should().Be("1");
            false.Convert(StoragesClasses.StorageClassType.IntegerClass).Should().Be("0");
            true.Convert(StoragesClasses.StorageClassType.RealClass).Should().Be("1");
            false.Convert(StoragesClasses.StorageClassType.RealClass).Should().Be("0");
            true.Convert(StoragesClasses.StorageClassType.NumericClass).Should().Be("1");
            false.Convert(StoragesClasses.StorageClassType.NumericClass).Should().Be("0");
            true.Convert(StoragesClasses.StorageClassType.TextClass).Should().Be("'True'");
            false.Convert(StoragesClasses.StorageClassType.TextClass).Should().Be("'False'");

            // Not Supported
            true.Invoking(x => x.Convert(StoragesClasses.StorageClassType.BlobClass)).Should()
                .Throw<NotSupportedException>();
            false.Invoking(x => x.Convert(StoragesClasses.StorageClassType.BlobClass)).Should()
                .Throw<NotSupportedException>();

            // Nullables
            bool? value = null;
            value.Convert(StoragesClasses.StorageClassType.IntegerClass).Should().Be("NULL");

            value = true;
            value.Convert(StoragesClasses.StorageClassType.IntegerClass).Should().Be("1");
        }

        [Fact]
        public void ByteConversionCorrect()
        {
            // Conversions
            const byte value = 1;
            value.Convert(StoragesClasses.StorageClassType.BlobClass).Should().Be("x'01'");
            value.Convert(StoragesClasses.StorageClassType.IntegerClass).Should().Be("1");
            value.Convert(StoragesClasses.StorageClassType.NumericClass).Should().Be("1");

            // Not Supported
            value.Invoking(x => x.Convert(StoragesClasses.StorageClassType.TextClass)).Should()
                .Throw<NotSupportedException>();

            // Nullables
            byte? value2 = null;
            value2.Convert(StoragesClasses.StorageClassType.IntegerClass).Should().Be("NULL");

            value2 = 1;
            value2.Convert(StoragesClasses.StorageClassType.IntegerClass).Should().Be("1");
        }

        [Fact]
        public void ByteArrayConversionCorrect()
        {
            // Conversions
            byte[] value = { 1, 2 };
            value.Convert(StoragesClasses.StorageClassType.BlobClass).Should().Be("x'0102'");

            // Not Supported
            value.Invoking(x => x.Convert(StoragesClasses.StorageClassType.TextClass)).Should()
                .Throw<NotSupportedException>();

            // Nullables
            byte[]? value2 = null;
            value2.Convert(StoragesClasses.StorageClassType.BlobClass).Should().Be("NULL");

            value2 = Array.Empty<byte>();
            value2.Convert(StoragesClasses.StorageClassType.BlobClass).Should().Be("NULL");

            value2 = new byte[] { 1, 2 };
            value2.Convert(StoragesClasses.StorageClassType.BlobClass).Should().Be("x'0102'");
        }

        [Fact]
        public void SbyteConversionCorrect()
        {
            // Conversions
            const sbyte value = 1;
            value.Convert(StoragesClasses.StorageClassType.BlobClass).Should().Be("x'01'");
            value.Convert(StoragesClasses.StorageClassType.IntegerClass).Should().Be("1");
            value.Convert(StoragesClasses.StorageClassType.NumericClass).Should().Be("1");

            // Not Supported
            value.Invoking(x => x.Convert(StoragesClasses.StorageClassType.TextClass)).Should()
                .Throw<NotSupportedException>();

            // Nullables
            sbyte? value2 = null;
            value2.Convert(StoragesClasses.StorageClassType.IntegerClass).Should().Be("NULL");

            value2 = 1;
            value2.Convert(StoragesClasses.StorageClassType.IntegerClass).Should().Be("1");
        }

        [Fact]
        public void SbyteArrayConversionCorrect()
        {
            // Conversions
            sbyte[] value = { 1, 2 };
            value.Convert(StoragesClasses.StorageClassType.BlobClass).Should().Be("x'0102'");

            // Not Supported
            value.Invoking(x => x.Convert(StoragesClasses.StorageClassType.TextClass)).Should()
                .Throw<NotSupportedException>();

            // Nullables
            sbyte[]? value2 = null;
            value2.Convert(StoragesClasses.StorageClassType.BlobClass).Should().Be("NULL");

            value2 = Array.Empty<sbyte>();
            value2.Convert(StoragesClasses.StorageClassType.BlobClass).Should().Be("NULL");

            value2 = new sbyte[] { 1, 2 };
            value2.Convert(StoragesClasses.StorageClassType.BlobClass).Should().Be("x'0102'");
        }

        [Fact]
        public void CharConversionCorrect()
        {
            // Conversions
            const char value = 'c';
            value.Convert(StoragesClasses.StorageClassType.TextClass).Should().Be("'c'");
            value.Convert(StoragesClasses.StorageClassType.IntegerClass).Should().Be("99");
            value.Convert(StoragesClasses.StorageClassType.NumericClass).Should().Be("99");

            // Not Supported
            value.Invoking(x => x.Convert(StoragesClasses.StorageClassType.BlobClass)).Should()
                .Throw<NotSupportedException>();

            // Nullables
            char? value2 = null;
            value2.Convert(StoragesClasses.StorageClassType.TextClass).Should().Be("NULL");

            value2 = 'c';
            value2.Convert(StoragesClasses.StorageClassType.TextClass).Should().Be("'c'");
        }

        [Fact]
        public void CharArrayConversionCorrect()
        {
            // Conversions
            char[] value = { 'H', 'e', 'l', 'l', 'o' };
            value.Convert(StoragesClasses.StorageClassType.TextClass).Should().Be("'Hello'");

            // Not Supported
            value.Invoking(x => x.Convert(StoragesClasses.StorageClassType.BlobClass)).Should()
                .Throw<NotSupportedException>();

            // Nullables
            char[]? value2 = null;
            value2.Convert(StoragesClasses.StorageClassType.TextClass).Should().Be("NULL");

            value2 = Array.Empty<char>();
            value2.Convert(StoragesClasses.StorageClassType.TextClass).Should().Be("''");

            value2 = new[] { 'H', 'e', 'l', 'l', 'o' };
            value2.Convert(StoragesClasses.StorageClassType.TextClass).Should().Be("'Hello'");
        }

        [Fact]
        public void DecimalConversionsCorrect()
        {
            // Conversions
            const decimal value = 1.5M;
            value.Convert(StoragesClasses.StorageClassType.TextClass).Should().Be("'1.5'");
            value.Convert(StoragesClasses.StorageClassType.NumericClass).Should().Be("1.5");
            value.Convert(StoragesClasses.StorageClassType.RealClass).Should().Be("1.5");
            value.Convert(StoragesClasses.StorageClassType.IntegerClass).Should().Be("2");

            // Not Supported
            value.Invoking(x => x.Convert(StoragesClasses.StorageClassType.BlobClass)).Should()
                .Throw<NotSupportedException>();

            // Nullables
            decimal? value2 = null;
            value2.Convert(StoragesClasses.StorageClassType.NumericClass).Should().Be("NULL");

            value2 = 1.5M;
            value2.Convert(StoragesClasses.StorageClassType.NumericClass).Should().Be("1.5");
        }

        [Fact]
        public void DoubleConversionsCorrect()
        {
            // Conversions
            const double value = 1.5D;
            value.Convert(StoragesClasses.StorageClassType.TextClass).Should().Be("'1.5'");
            value.Convert(StoragesClasses.StorageClassType.NumericClass).Should().Be("1.5");
            value.Convert(StoragesClasses.StorageClassType.RealClass).Should().Be("1.5");
            value.Convert(StoragesClasses.StorageClassType.IntegerClass).Should().Be("2");

            // Not Supported
            value.Invoking(x => x.Convert(StoragesClasses.StorageClassType.BlobClass)).Should()
                .Throw<NotSupportedException>();

            // Nullables
            double? value2 = null;
            value2.Convert(StoragesClasses.StorageClassType.NumericClass).Should().Be("NULL");

            value2 = 1.5D;
            value2.Convert(StoragesClasses.StorageClassType.NumericClass).Should().Be("1.5");
        }

        [Fact]
        public void FloatConversionsCorrect()
        {
            // Conversions
            const float value = 1.5F;
            value.Convert(StoragesClasses.StorageClassType.TextClass).Should().Be("'1.5'");
            value.Convert(StoragesClasses.StorageClassType.NumericClass).Should().Be("1.5");
            value.Convert(StoragesClasses.StorageClassType.RealClass).Should().Be("1.5");
            value.Convert(StoragesClasses.StorageClassType.IntegerClass).Should().Be("2");

            // Not Supported
            value.Invoking(x => x.Convert(StoragesClasses.StorageClassType.BlobClass)).Should()
                .Throw<NotSupportedException>();

            // Nullables
            float? value2 = null;
            value2.Convert(StoragesClasses.StorageClassType.NumericClass).Should().Be("NULL");

            value2 = 1.5F;
            value2.Convert(StoragesClasses.StorageClassType.NumericClass).Should().Be("1.5");
        }

        [Fact]
        public void IntConversionsCorrect()
        {
            // Conversions
            const int value = 5;
            value.Convert(StoragesClasses.StorageClassType.TextClass).Should().Be("'5'");
            value.Convert(StoragesClasses.StorageClassType.NumericClass).Should().Be("5");
            value.Convert(StoragesClasses.StorageClassType.RealClass).Should().Be("5");
            value.Convert(StoragesClasses.StorageClassType.IntegerClass).Should().Be("5");

            // Not Supported
            value.Invoking(x => x.Convert(StoragesClasses.StorageClassType.BlobClass)).Should()
                .Throw<NotSupportedException>();

            // Nullables
            int? value2 = null;
            value2.Convert(StoragesClasses.StorageClassType.NumericClass).Should().Be("NULL");

            value2 = 5;
            value2.Convert(StoragesClasses.StorageClassType.NumericClass).Should().Be("5");
        }

        [Fact]
        public void UintConversionsCorrect()
        {
            // Conversions
            const uint value = 5;
            value.Convert(StoragesClasses.StorageClassType.TextClass).Should().Be("'5'");
            value.Convert(StoragesClasses.StorageClassType.NumericClass).Should().Be("5");
            value.Convert(StoragesClasses.StorageClassType.RealClass).Should().Be("5");
            value.Convert(StoragesClasses.StorageClassType.IntegerClass).Should().Be("5");

            // Not Supported
            value.Invoking(x => x.Convert(StoragesClasses.StorageClassType.BlobClass)).Should()
                .Throw<NotSupportedException>();

            // Nullables
            uint? value2 = null;
            value2.Convert(StoragesClasses.StorageClassType.NumericClass).Should().Be("NULL");

            value2 = 5;
            value2.Convert(StoragesClasses.StorageClassType.NumericClass).Should().Be("5");
        }

        [Fact]
        public void LongConversionsCorrect()
        {
            // Conversions
            const long value = 5;
            value.Convert(StoragesClasses.StorageClassType.TextClass).Should().Be("'5'");
            value.Convert(StoragesClasses.StorageClassType.NumericClass).Should().Be("5");
            value.Convert(StoragesClasses.StorageClassType.RealClass).Should().Be("5");
            value.Convert(StoragesClasses.StorageClassType.IntegerClass).Should().Be("5");

            // Not Supported
            value.Invoking(x => x.Convert(StoragesClasses.StorageClassType.BlobClass)).Should()
                .Throw<NotSupportedException>();

            // Nullables
            long? value2 = null;
            value2.Convert(StoragesClasses.StorageClassType.NumericClass).Should().Be("NULL");

            value2 = 5;
            value2.Convert(StoragesClasses.StorageClassType.NumericClass).Should().Be("5");
        }

        [Fact]
        public void UlongConversionsCorrect()
        {
            // Conversions
            const ulong value = 5;
            value.Convert(StoragesClasses.StorageClassType.TextClass).Should().Be("'5'");
            value.Convert(StoragesClasses.StorageClassType.NumericClass).Should().Be("5");
            value.Convert(StoragesClasses.StorageClassType.RealClass).Should().Be("5");
            value.Convert(StoragesClasses.StorageClassType.IntegerClass).Should().Be("5");

            // Not Supported
            value.Invoking(x => x.Convert(StoragesClasses.StorageClassType.BlobClass)).Should()
                .Throw<NotSupportedException>();

            // Nullables
            ulong? value2 = null;
            value2.Convert(StoragesClasses.StorageClassType.NumericClass).Should().Be("NULL");

            value2 = 5;
            value2.Convert(StoragesClasses.StorageClassType.NumericClass).Should().Be("5");
        }

        [Fact]
        public void ShortConversionsCorrect()
        {
            // Conversions
            const short value = 5;
            value.Convert(StoragesClasses.StorageClassType.TextClass).Should().Be("'5'");
            value.Convert(StoragesClasses.StorageClassType.NumericClass).Should().Be("5");
            value.Convert(StoragesClasses.StorageClassType.RealClass).Should().Be("5");
            value.Convert(StoragesClasses.StorageClassType.IntegerClass).Should().Be("5");

            // Not Supported
            value.Invoking(x => x.Convert(StoragesClasses.StorageClassType.BlobClass)).Should()
                .Throw<NotSupportedException>();

            // Nullables
            short? value2 = null;
            value2.Convert(StoragesClasses.StorageClassType.NumericClass).Should().Be("NULL");

            value2 = 5;
            value2.Convert(StoragesClasses.StorageClassType.NumericClass).Should().Be("5");
        }

        [Fact]
        public void UShortConversionsCorrect()
        {
            // Conversions
            const ushort value = 5;
            value.Convert(StoragesClasses.StorageClassType.TextClass).Should().Be("'5'");
            value.Convert(StoragesClasses.StorageClassType.NumericClass).Should().Be("5");
            value.Convert(StoragesClasses.StorageClassType.RealClass).Should().Be("5");
            value.Convert(StoragesClasses.StorageClassType.IntegerClass).Should().Be("5");

            // Not Supported
            value.Invoking(x => x.Convert(StoragesClasses.StorageClassType.BlobClass)).Should()
                .Throw<NotSupportedException>();

            // Nullables
            ushort? value2 = null;
            value2.Convert(StoragesClasses.StorageClassType.NumericClass).Should().Be("NULL");

            value2 = 5;
            value2.Convert(StoragesClasses.StorageClassType.NumericClass).Should().Be("5");
        }

        [Fact]
        public void StringConversionsCorrect()
        {
            // Conversions

            //   Basic
            var value = "ab'cd";
            value.Convert(StoragesClasses.StorageClassType.TextClass).Should().Be("'ab''cd'");

            //   LiteralNullIsNull
            value = "null";
            value.Convert(StoragesClasses.StorageClassType.TextClass, StringValueInterpretation.LiteralNullIsNull)
                .Should().Be("NULL");

            //   EmptyStringIsNull
            value = string.Empty;
            value.Convert(StoragesClasses.StorageClassType.TextClass, StringValueInterpretation.EmptyStringIsNull)
                .Should().Be("NULL");

            //   StripAlpha
            value = " £10.99 in total";
            value.Convert(StoragesClasses.StorageClassType.NumericClass, StringValueInterpretation.StripAlpha)
                .Should().Be("10.99");

            value = "/11.22.33/";
            value.Invoking(x => x.Convert(StoragesClasses.StorageClassType.NumericClass, StringValueInterpretation.StripAlpha))
                .Should().Throw<ArgumentException>();

            //   LocalDate
            System.Threading.Thread.CurrentThread.CurrentCulture = new CultureInfo("en-GB");
            value = "02/07/2022";
            value.Convert(StoragesClasses.StorageClassType.TextClass, StringValueInterpretation.LocalDate)
                .Should().Be("'2022-07-02T00:00:00.0000000'");

            //   Integer
            value = "1";
            value.Convert(StoragesClasses.StorageClassType.IntegerClass)
                .Should().Be("1");

            //   Numeric
            value = "1.5";
            value.Convert(StoragesClasses.StorageClassType.NumericClass)
                .Should().Be("1.5");

            //   Real
            value = "1.5";
            value.Convert(StoragesClasses.StorageClassType.RealClass)
                .Should().Be("1.5");

            //   Base64
            value = "AQID";
            value.Convert(StoragesClasses.StorageClassType.BlobClass, StringValueInterpretation.Base64).Should()
                .Be("x'010203'");

            value = "base64:AQID";
            value.Convert(StoragesClasses.StorageClassType.BlobClass).Should()
                .Be("x'010203'");

            //   Hex
            value = "0x010203";
            value.Convert(StoragesClasses.StorageClassType.BlobClass, StringValueInterpretation.Hex).Should()
                .Be("x'010203'");

            //   UTF8 bytes
            value = "ABC";
            value.Convert(StoragesClasses.StorageClassType.BlobClass).Should()
                .Be("x'414243'");

            // Not Supported
            //   None

            // Nullables
            string? value2 = null;
            value2.Convert(StoragesClasses.StorageClassType.TextClass).Should().Be("NULL");

            value2 = "foo";
            value2.Convert(StoragesClasses.StorageClassType.TextClass).Should().Be("'foo'");
        }

        [Fact]
        public void DateTimeConversionsSuccessful()
        {
            // Conversions
            var value = new DateTime(2022, 7, 2);
            value.Convert(StoragesClasses.StorageClassType.TextClass).Should().Be("'2022-07-02T00:00:00.0000000'");
            value.Convert(StoragesClasses.StorageClassType.IntegerClass).Should().Be("1656720000");
            value.Convert(StoragesClasses.StorageClassType.NumericClass).Should().Be("1656720000");

            // Not Supported
            value.Invoking(x => x.Convert(StoragesClasses.StorageClassType.BlobClass)).Should()
                .Throw<NotSupportedException>();

            // Nullables
            DateTime? value2 = null;
            value2.Convert(StoragesClasses.StorageClassType.TextClass).Should().Be("NULL");

            value2 = new DateTime(2022, 7, 2);
            value2.Convert(StoragesClasses.StorageClassType.TextClass).Should().Be("'2022-07-02T00:00:00.0000000'");
        }

        [Fact]
        public void DateTimeOffsetConversionsSuccessful()
        {
            // Conversions
            var value = (DateTimeOffset)new DateTime(2022, 7, 2, 0, 0, 0, DateTimeKind.Utc);
            value.Convert(StoragesClasses.StorageClassType.TextClass).Should().Be("'2022-07-02T00:00:00.0000000+00:00'");
            value.Convert(StoragesClasses.StorageClassType.IntegerClass).Should().Be("1656720000");
            value.Convert(StoragesClasses.StorageClassType.NumericClass).Should().Be("1656720000");

            // Not Supported
            value.Invoking(x => x.Convert(StoragesClasses.StorageClassType.BlobClass)).Should()
                .Throw<NotSupportedException>();

            // Nullables
            DateTimeOffset? value2 = null;
            value2.Convert(StoragesClasses.StorageClassType.TextClass).Should().Be("NULL");

            value2 = new DateTime(2022, 7, 2, 0, 0, 0, DateTimeKind.Utc);
            value2.Convert(StoragesClasses.StorageClassType.TextClass).Should().Be("'2022-07-02T00:00:00.0000000+00:00'");
        }

        [Fact]
        public void GuidConversionsSuccessful()
        {
            // Conversions
            var value = Guid.Parse("eb755d9b-faaf-423a-9a2c-1e42ee9a3ef8");
            value.Convert(StoragesClasses.StorageClassType.TextClass).Should()
                .Be("'eb755d9b-faaf-423a-9a2c-1e42ee9a3ef8'");

            // Not Supported
            value.Invoking(x => x.Convert(StoragesClasses.StorageClassType.BlobClass)).Should()
                .Throw<NotSupportedException>();

            // Nullables
            Guid? value2 = null;
            value2.Convert(StoragesClasses.StorageClassType.TextClass).Should().Be("NULL");

            value2 = Guid.Parse("eb755d9b-faaf-423a-9a2c-1e42ee9a3ef8");
            value2.Convert(StoragesClasses.StorageClassType.TextClass).Should()
                .Be("'eb755d9b-faaf-423a-9a2c-1e42ee9a3ef8'");
        }

        [Fact]
        public void JObjectConversionsSuccessful()
        {
            // Conversions
            var value = JObject.Parse("{ \"foo\": \"bar\" }");
            value.Convert(StoragesClasses.StorageClassType.TextClass).Should().Be("'{\"foo\":\"bar\"}'");

            // Not Supported
            value.Invoking(x => x.Convert(StoragesClasses.StorageClassType.BlobClass)).Should()
                .Throw<NotSupportedException>();

            // Nullables
            JObject? value2 = null;
            value2.Convert(StoragesClasses.StorageClassType.TextClass).Should().Be("NULL");

            value2 = JObject.Parse("{ \"foo\": \"bar\" }");
            value2.Convert(StoragesClasses.StorageClassType.TextClass).Should().Be("'{\"foo\":\"bar\"}'");
        }

        [Fact]
        public void JArrayConversionsSuccessful()
        {
            // Conversions
            var value = JArray.Parse("[ 1, 2, 3 ]");
            value.Convert(StoragesClasses.StorageClassType.TextClass).Should().Be("'[1,2,3]'");

            // Not Supported
            value.Invoking(x => x.Convert(StoragesClasses.StorageClassType.BlobClass)).Should()
                .Throw<NotSupportedException>();

            // Nullables
            JArray? value2 = null;
            value2.Convert(StoragesClasses.StorageClassType.TextClass).Should().Be("NULL");

            value2 = JArray.Parse("[ 1, 2, 3 ]");
            value2.Convert(StoragesClasses.StorageClassType.TextClass).Should().Be("'[1,2,3]'");
        }

        [Fact]
        public void NotSupportedRejected()
        {
            var value = new SqliteConnection();
            value.Invoking(x => x.Convert(StoragesClasses.StorageClassType.BlobClass)).Should()
                .Throw<NotSupportedException>();
        }
    }
}
