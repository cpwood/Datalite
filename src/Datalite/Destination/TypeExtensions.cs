using System;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using Datalite.Sources;
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
        /// For a generic <see cref="Object"/>, convert the
        /// value to a token that can be used in a SQL INSERT statement according to the
        /// <see cref="StoragesClasses.StorageClassType"/>.
        /// </summary>
        /// <param name="value">The original value.</param>
        /// <param name="to">The <see cref="StoragesClasses.StorageClassType"/> that indicates how the value will be stored.</param>
        /// <param name="interpretation">Explains how to deal with string data that might be ambiguous.</param>
        /// <returns></returns>
        /// <exception cref="NotSupportedException"></exception>
        public static string Convert(this object? value, 
            StoragesClasses.StorageClassType to, 
            StringValueInterpretation interpretation = StringValueInterpretation.Default)
        {
            return value.Convert(value?.GetType() ?? typeof(object), to, interpretation);
        }

        /// <summary>
        /// For a generic <see cref="Object"/>, specify the source <see cref="Type"/> and convert the
        /// value to a token that can be used in a SQL INSERT statement according to the
        /// <see cref="StoragesClasses.StorageClassType"/>.
        /// </summary>
        /// <param name="value">The original value.</param>
        /// <param name="from">How the <see cref="object"/> value should be read.</param>
        /// <param name="to">The <see cref="StoragesClasses.StorageClassType"/> that indicates how the value will be stored.</param>
        /// <param name="interpretation">Explains how to deal with string data that might be ambiguous.</param>
        /// <returns></returns>
        /// <exception cref="NotSupportedException"></exception>
        public static string Convert(this object? value, 
            Type from, 
            StoragesClasses.StorageClassType to, 
            StringValueInterpretation interpretation = StringValueInterpretation.Default)
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

            throw new NotSupportedException($"Cannot convert from a {from.Name} to a {to}.");
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
                _ => throw new NotSupportedException($"Cannot convert from a bool to a {type}."),
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
                _ => throw new NotSupportedException($"Cannot convert from a byte to a {type}.")
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
                _ => throw new NotSupportedException($"Cannot convert from a byte array to a {type}.")
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
                _ => throw new NotSupportedException($"Cannot convert from a sbyte to a {type}.")
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
                _ => throw new NotSupportedException($"Cannot convert from an sbyte array to a {type}.")
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
                _ => throw new NotSupportedException($"Cannot convert from a char to a {type}.")
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
                _ => throw new NotSupportedException($"Cannot convert from a char array to a {type}.")
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
                _ => throw new NotSupportedException($"Cannot convert from a decimal to a {type}.")
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
                _ => throw new NotSupportedException($"Cannot convert from a double to a {type}.")
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
                _ => throw new NotSupportedException($"Cannot convert from a float to a {type}.")
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
                _ => throw new NotSupportedException($"Cannot convert from an int to a {type}.")
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
                _ => throw new NotSupportedException($"Cannot convert from a uint to a {type}.")
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
                _ => throw new NotSupportedException($"Cannot convert from a long to a {type}.")
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
                _ => throw new NotSupportedException($"Cannot convert from a ulong to a {type}.")
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
                _ => throw new NotSupportedException($"Cannot convert from a short to a {type}.")
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
                _ => throw new NotSupportedException($"Cannot convert from a ushort to a {type}.")
            };
        }

        /// <summary>
        /// Convert the value to a token that can be used in a SQL INSERT statement according to the
        /// <see cref="StoragesClasses.StorageClassType"/>.
        /// </summary>
        /// <param name="value">The original value.</param>
        /// <param name="type">The <see cref="StoragesClasses.StorageClassType"/> that indicates how the value will be stored.</param>
        /// <param name="interpretation">Explains how to deal with string data that might be ambiguous.</param>
        /// <returns></returns>
        /// <exception cref="NotSupportedException"></exception>
        public static string As(this string value, 
            StoragesClasses.StorageClassType type, 
            StringValueInterpretation interpretation = StringValueInterpretation.Default)
        {
            if (interpretation.HasFlag(StringValueInterpretation.LiteralNullIsNull) && value.ToLowerInvariant() == "null")
                return "NULL";

            if (interpretation.HasFlag(StringValueInterpretation.EmptyStringIsNull) && string.IsNullOrEmpty(value))
                return "NULL";

            if (interpretation.HasFlag(StringValueInterpretation.StripAlpha))
            {
                value = Regex.Replace(value,
                    $"[^0-9\\{CultureInfo.CurrentCulture.NumberFormat.CurrencyDecimalSeparator}\\{CultureInfo.CurrentCulture.NumberFormat.NegativeSign}]",
                    string.Empty);

                if (!decimal.TryParse(value, out _))
                    throw new ArgumentException($"Cannot convert the string '{value}' to a number.");
            }

            if (interpretation.HasFlag(StringValueInterpretation.LocalDate))
                value = DateTime.Parse(value).ToString("o");

            return type switch
            {
                StoragesClasses.StorageClassType.IntegerClass => value,
                StoragesClasses.StorageClassType.NumericClass => value,
                StoragesClasses.StorageClassType.RealClass => value,
                StoragesClasses.StorageClassType.BlobClass => GenerateBytesOutputString(value, interpretation),
                StoragesClasses.StorageClassType.TextClass => $"'{value.Replace("'", "''")}'",
                _ => throw new NotSupportedException($"Cannot convert from a string to a {type}.")
            };
        }

        private static string GenerateBytesOutputString(string value, StringValueInterpretation interpretation)
        {
            if (interpretation.HasFlag(StringValueInterpretation.Base64))
                return $"x'{ToHexString(System.Convert.FromBase64String(value))}'";

            if (interpretation.HasFlag(StringValueInterpretation.Hex))
                return $"x'{ToHexString(FromHexString(value))}'";

            if (value.StartsWith("base64:"))
                return $"x'{ToHexString(System.Convert.FromBase64String(value[7..]))}'";

            return $"x'{Encoding.UTF8.GetBytes(value)}'";
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
                _ => throw new NotSupportedException($"Cannot convert from a DateTime to a {type}.")
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
                _ => throw new NotSupportedException($"Cannot convert from a DateTimeOffset to a {type}.")
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

            throw new NotSupportedException($"Cannot convert from a Guid to a {type}.");
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

            throw new NotSupportedException($"Cannot convert from a JObject to a {type}.");
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

            throw new NotSupportedException($"Cannot convert from a JArray to a {type}.");
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

        /// <summary>
        /// Converts a hex string to a byte array.
        /// </summary>
        /// <param name="hex">The hex string.</param>
        /// <returns>A byte array.</returns>
        static byte[] FromHexString(string hex)
        {
            if (hex.Contains('x'))
                hex = hex.Substring(hex.IndexOf('x') + 1);

            hex = Regex.Replace(hex, "[^0-9a-f]", string.Empty, RegexOptions.IgnoreCase);

            if (hex.Length % 2 != 0)
                throw new ArgumentException("The hex value does not have a valid length.");
                    
            var bytes = new byte[hex.Length / 2];

            for (var i = 0; i < hex.Length; i = i + 2)
            {
                bytes[i / 2] = byte.Parse(hex.Substring(i, 2), NumberStyles.HexNumber);
            }

            return bytes;
        }
    }
}
