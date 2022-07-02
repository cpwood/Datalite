using System;
using System.Data;
using Datalite.Destination;

namespace Datalite.Sources
{
    /// <summary>
    /// Extension methods for <see cref="IDataReader"/>.
    /// </summary>
    public static class DataReaderExtensions
    {
        /// <summary>
        /// For a value in an <see cref="IDataReader"/>, get its string representation
        /// so that it can be included in an INSERT script. For example, string values
        /// will be quoted.
        /// </summary>
        /// <param name="reader">The <see cref="IDataReader"/> containing the data being read.</param>
        /// <param name="column">The <see cref="Column"/> that describes that target field.</param>
        /// <returns>
        /// The string representation of the value, formatted so that it can be
        /// included within an INSERT script.
        /// </returns>
        /// <exception cref="NotSupportedException"></exception>
        public static string GetSqlValue(this IDataReader reader, Column column)
        {
            var position = reader.GetOrdinal(column.Name);
            var type = reader.GetFieldType(position);

            if (type == typeof(bool)) return reader.GetBoolean(position).As(column.StorageClass);
            if (type == typeof(byte)) return reader.GetByte(position).As(column.StorageClass);
            if (type == typeof(sbyte)) return reader.GetByte(position).As(column.StorageClass);
            if (type == typeof(char)) return reader.GetChar(position).As(column.StorageClass);
            if (type == typeof(decimal)) return reader.GetDecimal(position).As(column.StorageClass);
            if (type == typeof(double)) return reader.GetDouble(position).As(column.StorageClass);
            if (type == typeof(float)) return reader.GetFloat(position).As(column.StorageClass);
            if (type == typeof(int)) return reader.GetInt32(position).As(column.StorageClass);
            if (type == typeof(uint)) return reader.GetInt32(position).As(column.StorageClass);
            if (type == typeof(long)) return reader.GetInt64(position).As(column.StorageClass);
            if (type == typeof(ulong)) return reader.GetInt64(position).As(column.StorageClass);
            if (type == typeof(short)) return reader.GetInt16(position).As(column.StorageClass);
            if (type == typeof(ushort)) return reader.GetInt16(position).As(column.StorageClass);
            if (type == typeof(string)) return reader.GetString(position).As(column.StorageClass, column.Interpretation);
            if (type == typeof(DateTime)) return reader.GetDateTime(position).As(column.StorageClass);
            if (type == typeof(Guid)) return reader.GetGuid(position).As(column.StorageClass);

            if (type == typeof(byte[])) return ((byte[])reader[position]).As(column.StorageClass);
            if (type == typeof(sbyte[])) return ((sbyte[])reader[position]).As(column.StorageClass);
            if (type == typeof(char[])) return ((char[])reader[position]).As(column.StorageClass);

            throw new NotSupportedException($"Column type of {type} is not supported.");
        }
    }
}