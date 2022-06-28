using System;
using System.Globalization;
using System.Linq;
using System.Text;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Datalite.Destination
{
    /// <summary>
    /// Extension methods for <see cref="Type"/> and <see cref="object"/> records.
    /// </summary>
    public static class TypeExtensions
    {
        /// <summary>
        /// For a generic <see cref="Object"/>, specify the source <see cref="Type"/> and convert the
        /// value to a token that can be used in a SQL INSERT statement according to the
        /// <see cref="StoragesClasses.StorageClassType"/>.
        /// </summary>
        /// <param name="value">The original value.</param>
        /// <param name="from">How the <see cref="object"/> value should be read.</param>
        /// <param name="to">The <see cref="StoragesClasses.StorageClassType"/> that indicates how the value will be stored.</param>
        /// <returns></returns>
        /// <exception cref="NotSupportedException"></exception>
        public static string Convert(this object? value, Type from, StoragesClasses.StorageClassType to)
        {
            if (value == null) return "NULL";
            if (value == DBNull.Value) return "NULL";

            if (from == typeof(bool)) return ((bool)value).As(to);
            if (from == typeof(byte)) return ((byte)value).As(to);
            if (from == typeof(byte[])) return ((byte[])value).As(to);
            if (from == typeof(sbyte)) return ((sbyte)value).As(to);
            if (from == typeof(sbyte[])) return ((sbyte[])value).As(to);
            if (from == typeof(char)) return ((char)value).As(to);
            if (from == typeof(char[])) return ((char[])value).As(to);
            if (from == typeof(decimal)) return ((decimal)value).As(to);
            if (from == typeof(double)) return ((double)value).As(to);
            if (from == typeof(float)) return ((float)value).As(to);
            if (from == typeof(int)) return ((int)value).As(to);
            if (from == typeof(uint)) return ((uint)value).As(to);
            if (from == typeof(long)) return ((long)value).As(to);
            if (from == typeof(ulong)) return ((ulong)value).As(to);
            if (from == typeof(short)) return ((short)value).As(to);
            if (from == typeof(ushort)) return ((ushort)value).As(to);
            if (from == typeof(string)) return ((string)value).As(to);
            if (from == typeof(DateTime)) return ((DateTime)value).As(to);
            if (from == typeof(DateTimeOffset)) return ((DateTimeOffset)value).As(to);
            if (from == typeof(Guid)) return ((Guid)value).As(to);
            if (from == typeof(JObject)) return ((JObject)value).As(to);
            if (from == typeof(JArray)) return ((JArray)value).As(to);

            throw new NotSupportedException();
        }

        /// <summary>
        /// Convert the value to a token that can be used in a SQL INSERT statement according to the
        /// <see cref="StoragesClasses.StorageClassType"/>.
        /// </summary>
        /// <param name="value">The original value.</param>
        /// <param name="type">The <see cref="StoragesClasses.StorageClassType"/> that indicates how the value will be stored.</param>
        /// <returns></returns>
        /// <exception cref="NotSupportedException"></exception>
        public static string As(this bool value, StoragesClasses.StorageClassType type)
        {
            return type switch
            {
                StoragesClasses.StorageClassType.IntegerClass => value ? "1" : "0",
                StoragesClasses.StorageClassType.RealClass => value ? "1" : "0",
                StoragesClasses.StorageClassType.NumericClass => value ? "1" : "0",
                StoragesClasses.StorageClassType.TextClass => $"'{value}'",
                _ => throw new NotSupportedException(),
            };
        }

        /// <summary>
        /// Convert the value to a token that can be used in a SQL INSERT statement according to the
        /// <see cref="StoragesClasses.StorageClassType"/>.
        /// </summary>
        /// <param name="value">The original value.</param>
        /// <param name="type">The <see cref="StoragesClasses.StorageClassType"/> that indicates how the value will be stored.</param>
        /// <returns></returns>
        /// <exception cref="NotSupportedException"></exception>
        public static string As(this byte value, StoragesClasses.StorageClassType type)
        {
            return type switch
            {
                StoragesClasses.StorageClassType.BlobClass => $"x'{ToHexString(new[] { value })}'",
                StoragesClasses.StorageClassType.IntegerClass => ((int)value).ToString(),
                StoragesClasses.StorageClassType.NumericClass => ((int)value).ToString(),
                _ => throw new NotSupportedException()
            };
        }

        /// <summary>
        /// Convert the value to a token that can be used in a SQL INSERT statement according to the
        /// <see cref="StoragesClasses.StorageClassType"/>.
        /// </summary>
        /// <param name="value">The original value.</param>
        /// <param name="type">The <see cref="StoragesClasses.StorageClassType"/> that indicates how the value will be stored.</param>
        /// <returns></returns>
        /// <exception cref="NotSupportedException"></exception>
        public static string As(this byte[] value, StoragesClasses.StorageClassType type)
        {
            return type switch
            {
                StoragesClasses.StorageClassType.BlobClass => $"x'{ToHexString(value)}'",
                _ => throw new NotSupportedException()
            };
        }

        /// <summary>
        /// Convert the value to a token that can be used in a SQL INSERT statement according to the
        /// <see cref="StoragesClasses.StorageClassType"/>.
        /// </summary>
        /// <param name="value">The original value.</param>
        /// <param name="type">The <see cref="StoragesClasses.StorageClassType"/> that indicates how the value will be stored.</param>
        /// <returns></returns>
        /// <exception cref="NotSupportedException"></exception>
        public static string As(this sbyte value, StoragesClasses.StorageClassType type)
        {
            return type switch
            {
                StoragesClasses.StorageClassType.BlobClass => $"x'{ToHexString(new[] { (byte)value })}'",
                StoragesClasses.StorageClassType.IntegerClass => ((int)value).ToString(),
                StoragesClasses.StorageClassType.NumericClass => ((int)value).ToString(),
                _ => throw new NotSupportedException()
            };
        }

        /// <summary>
        /// Convert the value to a token that can be used in a SQL INSERT statement according to the
        /// <see cref="StoragesClasses.StorageClassType"/>.
        /// </summary>
        /// <param name="value">The original value.</param>
        /// <param name="type">The <see cref="StoragesClasses.StorageClassType"/> that indicates how the value will be stored.</param>
        /// <returns></returns>
        /// <exception cref="NotSupportedException"></exception>
        public static string As(this sbyte[] value, StoragesClasses.StorageClassType type)
        {
            return type switch
            {
                StoragesClasses.StorageClassType.BlobClass =>
                    $"x'{ToHexString(value.Select(x => (byte)x).ToArray())}'",
                _ => throw new NotSupportedException()
            };
        }

        /// <summary>
        /// Convert the value to a token that can be used in a SQL INSERT statement according to the
        /// <see cref="StoragesClasses.StorageClassType"/>.
        /// </summary>
        /// <param name="value">The original value.</param>
        /// <param name="type">The <see cref="StoragesClasses.StorageClassType"/> that indicates how the value will be stored.</param>
        /// <returns></returns>
        /// <exception cref="NotSupportedException"></exception>
        public static string As(this char value, StoragesClasses.StorageClassType type)
        {
            return type switch
            {
                StoragesClasses.StorageClassType.TextClass => $"'{value}'",
                StoragesClasses.StorageClassType.IntegerClass => ((int)value).ToString(),
                StoragesClasses.StorageClassType.NumericClass => ((int)value).ToString(),
                _ => throw new NotSupportedException()
            };
        }

        /// <summary>
        /// Convert the value to a token that can be used in a SQL INSERT statement according to the
        /// <see cref="StoragesClasses.StorageClassType"/>.
        /// </summary>
        /// <param name="value">The original value.</param>
        /// <param name="type">The <see cref="StoragesClasses.StorageClassType"/> that indicates how the value will be stored.</param>
        /// <returns></returns>
        /// <exception cref="NotSupportedException"></exception>
        public static string As(this char[] value, StoragesClasses.StorageClassType type)
        {
            return type switch
            {
                StoragesClasses.StorageClassType.TextClass => $"'{value}'",
                _ => throw new NotSupportedException()
            };
        }

        /// <summary>
        /// Convert the value to a token that can be used in a SQL INSERT statement according to the
        /// <see cref="StoragesClasses.StorageClassType"/>.
        /// </summary>
        /// <param name="value">The original value.</param>
        /// <param name="type">The <see cref="StoragesClasses.StorageClassType"/> that indicates how the value will be stored.</param>
        /// <returns></returns>
        /// <exception cref="NotSupportedException"></exception>
        public static string As(this decimal value, StoragesClasses.StorageClassType type)
        {
            return type switch
            {
                StoragesClasses.StorageClassType.TextClass => $"'{value}'",
                StoragesClasses.StorageClassType.NumericClass => value.ToString(CultureInfo.InvariantCulture),
                StoragesClasses.StorageClassType.RealClass => value.ToString(CultureInfo.InvariantCulture),
                StoragesClasses.StorageClassType.IntegerClass => ((long)Math.Round(value, 0)).ToString(),
                _ => throw new NotSupportedException()
            };
        }

        /// <summary>
        /// Convert the value to a token that can be used in a SQL INSERT statement according to the
        /// <see cref="StoragesClasses.StorageClassType"/>.
        /// </summary>
        /// <param name="value">The original value.</param>
        /// <param name="type">The <see cref="StoragesClasses.StorageClassType"/> that indicates how the value will be stored.</param>
        /// <returns></returns>
        /// <exception cref="NotSupportedException"></exception>
        public static string As(this double value, StoragesClasses.StorageClassType type)
        {
            return type switch
            {
                StoragesClasses.StorageClassType.TextClass => $"'{value}'",
                StoragesClasses.StorageClassType.NumericClass => value.ToString(CultureInfo.InvariantCulture),
                StoragesClasses.StorageClassType.RealClass => value.ToString(CultureInfo.InvariantCulture),
                StoragesClasses.StorageClassType.IntegerClass => ((long)Math.Round(value, 0)).ToString(),
                _ => throw new NotSupportedException()
            };
        }

        /// <summary>
        /// Convert the value to a token that can be used in a SQL INSERT statement according to the
        /// <see cref="StoragesClasses.StorageClassType"/>.
        /// </summary>
        /// <param name="value">The original value.</param>
        /// <param name="type">The <see cref="StoragesClasses.StorageClassType"/> that indicates how the value will be stored.</param>
        /// <returns></returns>
        /// <exception cref="NotSupportedException"></exception>
        public static string As(this float value, StoragesClasses.StorageClassType type)
        {
            return type switch
            {
                StoragesClasses.StorageClassType.TextClass => $"'{value}'",
                StoragesClasses.StorageClassType.NumericClass => value.ToString(CultureInfo.InvariantCulture),
                StoragesClasses.StorageClassType.RealClass => value.ToString(CultureInfo.InvariantCulture),
                StoragesClasses.StorageClassType.IntegerClass => ((long)Math.Round(value, 0)).ToString(),
                _ => throw new NotSupportedException()
            };
        }

        /// <summary>
        /// Convert the value to a token that can be used in a SQL INSERT statement according to the
        /// <see cref="StoragesClasses.StorageClassType"/>.
        /// </summary>
        /// <param name="value">The original value.</param>
        /// <param name="type">The <see cref="StoragesClasses.StorageClassType"/> that indicates how the value will be stored.</param>
        /// <returns></returns>
        /// <exception cref="NotSupportedException"></exception>
        public static string As(this int value, StoragesClasses.StorageClassType type)
        {
            return type switch
            {
                StoragesClasses.StorageClassType.TextClass => $"'{value}'",
                StoragesClasses.StorageClassType.NumericClass => value.ToString(),
                StoragesClasses.StorageClassType.RealClass => value.ToString(),
                StoragesClasses.StorageClassType.IntegerClass => value.ToString(),
                _ => throw new NotSupportedException()
            };
        }

        /// <summary>
        /// Convert the value to a token that can be used in a SQL INSERT statement according to the
        /// <see cref="StoragesClasses.StorageClassType"/>.
        /// </summary>
        /// <param name="value">The original value.</param>
        /// <param name="type">The <see cref="StoragesClasses.StorageClassType"/> that indicates how the value will be stored.</param>
        /// <returns></returns>
        /// <exception cref="NotSupportedException"></exception>
        public static string As(this uint value, StoragesClasses.StorageClassType type)
        {
            return type switch
            {
                StoragesClasses.StorageClassType.TextClass => $"'{value}'",
                StoragesClasses.StorageClassType.NumericClass => value.ToString(),
                StoragesClasses.StorageClassType.RealClass => value.ToString(),
                StoragesClasses.StorageClassType.IntegerClass => value.ToString(),
                _ => throw new NotSupportedException()
            };
        }

        /// <summary>
        /// Convert the value to a token that can be used in a SQL INSERT statement according to the
        /// <see cref="StoragesClasses.StorageClassType"/>.
        /// </summary>
        /// <param name="value">The original value.</param>
        /// <param name="type">The <see cref="StoragesClasses.StorageClassType"/> that indicates how the value will be stored.</param>
        /// <returns></returns>
        /// <exception cref="NotSupportedException"></exception>
        public static string As(this long value, StoragesClasses.StorageClassType type)
        {
            return type switch
            {
                StoragesClasses.StorageClassType.TextClass => $"'{value}'",
                StoragesClasses.StorageClassType.NumericClass => value.ToString(),
                StoragesClasses.StorageClassType.RealClass => value.ToString(),
                StoragesClasses.StorageClassType.IntegerClass => value.ToString(),
                _ => throw new NotSupportedException()
            };
        }

        /// <summary>
        /// Convert the value to a token that can be used in a SQL INSERT statement according to the
        /// <see cref="StoragesClasses.StorageClassType"/>.
        /// </summary>
        /// <param name="value">The original value.</param>
        /// <param name="type">The <see cref="StoragesClasses.StorageClassType"/> that indicates how the value will be stored.</param>
        /// <returns></returns>
        /// <exception cref="NotSupportedException"></exception>
        public static string As(this ulong value, StoragesClasses.StorageClassType type)
        {
            return type switch
            {
                StoragesClasses.StorageClassType.TextClass => $"'{value}'",
                StoragesClasses.StorageClassType.NumericClass => value.ToString(),
                StoragesClasses.StorageClassType.RealClass => value.ToString(),
                StoragesClasses.StorageClassType.IntegerClass => value.ToString(),
                _ => throw new NotSupportedException()
            };
        }

        /// <summary>
        /// Convert the value to a token that can be used in a SQL INSERT statement according to the
        /// <see cref="StoragesClasses.StorageClassType"/>.
        /// </summary>
        /// <param name="value">The original value.</param>
        /// <param name="type">The <see cref="StoragesClasses.StorageClassType"/> that indicates how the value will be stored.</param>
        /// <returns></returns>
        /// <exception cref="NotSupportedException"></exception>
        public static string As(this short value, StoragesClasses.StorageClassType type)
        {
            return type switch
            {
                StoragesClasses.StorageClassType.TextClass => $"'{value}'",
                StoragesClasses.StorageClassType.NumericClass => value.ToString(),
                StoragesClasses.StorageClassType.RealClass => value.ToString(),
                StoragesClasses.StorageClassType.IntegerClass => value.ToString(),
                _ => throw new NotSupportedException()
            };
        }

        /// <summary>
        /// Convert the value to a token that can be used in a SQL INSERT statement according to the
        /// <see cref="StoragesClasses.StorageClassType"/>.
        /// </summary>
        /// <param name="value">The original value.</param>
        /// <param name="type">The <see cref="StoragesClasses.StorageClassType"/> that indicates how the value will be stored.</param>
        /// <returns></returns>
        /// <exception cref="NotSupportedException"></exception>
        public static string As(this ushort value, StoragesClasses.StorageClassType type)
        {
            return type switch
            {
                StoragesClasses.StorageClassType.TextClass => $"'{value}'",
                StoragesClasses.StorageClassType.NumericClass => value.ToString(),
                StoragesClasses.StorageClassType.RealClass => value.ToString(),
                StoragesClasses.StorageClassType.IntegerClass => value.ToString(),
                _ => throw new NotSupportedException()
            };
        }

        /// <summary>
        /// Convert the value to a token that can be used in a SQL INSERT statement according to the
        /// <see cref="StoragesClasses.StorageClassType"/>.
        /// </summary>
        /// <param name="value">The original value.</param>
        /// <param name="type">The <see cref="StoragesClasses.StorageClassType"/> that indicates how the value will be stored.</param>
        /// <returns></returns>
        /// <exception cref="NotSupportedException"></exception>
        public static string As(this string value, StoragesClasses.StorageClassType type)
        {
            return type switch
            {
                StoragesClasses.StorageClassType.BlobClass =>
                    $"x'{ToHexString(value.StartsWith("base64:") ? System.Convert.FromBase64String(value[7..]) : Encoding.UTF8.GetBytes(value))}'",
                StoragesClasses.StorageClassType.TextClass => $"'{value.Replace("'", "''")}'",
                _ => throw new NotSupportedException()
            };
        }

        /// <summary>
        /// Convert the value to a token that can be used in a SQL INSERT statement according to the
        /// <see cref="StoragesClasses.StorageClassType"/>.
        /// </summary>
        /// <param name="value">The original value.</param>
        /// <param name="type">The <see cref="StoragesClasses.StorageClassType"/> that indicates how the value will be stored.</param>
        /// <returns></returns>
        /// <exception cref="NotSupportedException"></exception>
        public static string As(this DateTime value, StoragesClasses.StorageClassType type)
        {
            return type switch
            {
                StoragesClasses.StorageClassType.IntegerClass => ((long)Math.Floor(DateTime.UtcNow.Subtract(value)
                    .TotalSeconds)).ToString(),
                StoragesClasses.StorageClassType.NumericClass => ((long)Math.Floor(DateTime.UtcNow.Subtract(value)
                    .TotalSeconds)).ToString(),
                StoragesClasses.StorageClassType.TextClass => $"'{value:o}'",
                _ => throw new NotSupportedException()
            };
        }

        /// <summary>
        /// Convert the value to a token that can be used in a SQL INSERT statement according to the
        /// <see cref="StoragesClasses.StorageClassType"/>.
        /// </summary>
        /// <param name="value">The original value.</param>
        /// <param name="type">The <see cref="StoragesClasses.StorageClassType"/> that indicates how the value will be stored.</param>
        /// <returns></returns>
        /// <exception cref="NotSupportedException"></exception>
        public static string As(this DateTimeOffset value, StoragesClasses.StorageClassType type)
        {
            return type switch
            {
                StoragesClasses.StorageClassType.IntegerClass => ((long)Math.Floor(DateTime.UtcNow
                    .Subtract(value.DateTime)
                    .TotalSeconds)).ToString(),
                StoragesClasses.StorageClassType.NumericClass => ((long)Math.Floor(DateTime.UtcNow
                    .Subtract(value.DateTime)
                    .TotalSeconds)).ToString(),
                StoragesClasses.StorageClassType.TextClass => $"'{value:o}'",
                _ => throw new NotSupportedException()
            };
        }

        /// <summary>
        /// Convert the value to a token that can be used in a SQL INSERT statement according to the
        /// <see cref="StoragesClasses.StorageClassType"/>.
        /// </summary>
        /// <param name="value">The original value.</param>
        /// <param name="type">The <see cref="StoragesClasses.StorageClassType"/> that indicates how the value will be stored.</param>
        /// <returns></returns>
        /// <exception cref="NotSupportedException"></exception>
        public static string As(this Guid value, StoragesClasses.StorageClassType type)
        {
            if (type == StoragesClasses.StorageClassType.TextClass)
                return $"'{value}'";

            throw new NotSupportedException();
        }

        /// <summary>
        /// Convert the value to a token that can be used in a SQL INSERT statement according to the
        /// <see cref="StoragesClasses.StorageClassType"/>.
        /// </summary>
        /// <param name="value">The original value.</param>
        /// <param name="type">The <see cref="StoragesClasses.StorageClassType"/> that indicates how the value will be stored.</param>
        /// <returns></returns>
        /// <exception cref="NotSupportedException"></exception>
        public static string As(this JObject value, StoragesClasses.StorageClassType type)
        {
            if (type == StoragesClasses.StorageClassType.TextClass)
                return $"'{value.ToString(Formatting.None).Replace("'", "''")}'";

            throw new NotSupportedException();
        }

        /// <summary>
        /// Convert the value to a token that can be used in a SQL INSERT statement according to the
        /// <see cref="StoragesClasses.StorageClassType"/>.
        /// </summary>
        /// <param name="value">The original value.</param>
        /// <param name="type">The <see cref="StoragesClasses.StorageClassType"/> that indicates how the value will be stored.</param>
        /// <returns></returns>
        /// <exception cref="NotSupportedException"></exception>
        public static string As(this JArray value, StoragesClasses.StorageClassType type)
        {
            if (type == StoragesClasses.StorageClassType.TextClass)
                return $"'{value.ToString(Formatting.None).Replace("'", "''")}'";

            throw new NotSupportedException();
        }

        /// <summary>
        /// Convert the bytes to a hexadecimal string.
        /// </summary>
        /// <param name="bytes">The bytes to convert.</param>
        /// <returns></returns>
        static string ToHexString(byte[] bytes)
        {
            var hexString = BitConverter.ToString(bytes);
            return hexString.Replace("-", "");
        }
    }
}
