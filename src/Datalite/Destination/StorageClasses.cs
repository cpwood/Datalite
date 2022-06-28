using System;
using Newtonsoft.Json.Linq;

namespace Datalite.Destination
{
    /// <summary>
    /// Sqlite Storage Classes and utility functions.
    /// </summary>
    public static class StoragesClasses
    {
        /// <summary>
        /// The Sqlite Storage Classes
        /// </summary>
        public enum StorageClassType
        {
            /// <summary>
            /// Integer
            /// </summary>
            IntegerClass,

            /// <summary>
            /// Real
            /// </summary>
            RealClass,

            /// <summary>
            /// Text
            /// </summary>
            TextClass,

            /// <summary>
            /// Blob
            /// </summary>
            BlobClass,

            /// <summary>
            /// Numeric
            /// </summary>
            NumericClass
        }

        /// <summary>
        /// Gets the DDL representation of a <see cref="StorageClassType"/> value.
        /// </summary>
        /// <param name="type">The <see cref="StorageClassType"/> instance.</param>
        /// <returns></returns>
        public static string AsString(this StorageClassType type)
        {
            return type switch
            {
                StorageClassType.IntegerClass => "INTEGER",
                StorageClassType.RealClass => "REAL",
                StorageClassType.TextClass => "TEXT",
                StorageClassType.BlobClass => "BLOB",
                StorageClassType.NumericClass => "NUMERIC",
                _ => "TEXT"
            };
        }

        /// <summary>
        /// Gets the <see cref="StorageClassType"/> representation of the DDL storage class sname.
        /// </summary>
        /// <param name="storageClassName">The DDL storage class name.</param>
        /// <returns></returns>
        public static StorageClassType GetStorageClassTypeFromName(string storageClassName)
        {
            return storageClassName switch
            {
                "INTEGER" => StorageClassType.IntegerClass,
                "REAL" => StorageClassType.RealClass,
                "TEXT" => StorageClassType.TextClass,
                "BLOB" => StorageClassType.BlobClass,
                "NUMERIC" => StorageClassType.NumericClass,
                _ => StorageClassType.TextClass
            };
        }

        /// <summary>
        /// Returns the <see cref="StorageClassType"/> associated with theCLR <see cref="Type"/>.
        /// </summary>
        /// <param name="type">The CLR <see cref="Type"/>.</param>
        /// <returns>The associated <see cref="StorageClassType" />.</returns>
        /// <exception cref="ArgumentOutOfRangeException"></exception>
        public static StorageClassType FromType(Type type)
        {
            if (type == typeof(bool)) return StorageClassType.IntegerClass;
            if (type == typeof(byte)) return StorageClassType.BlobClass;
            if (type == typeof(byte[])) return StorageClassType.BlobClass;
            if (type == typeof(sbyte)) return StorageClassType.BlobClass;
            if (type == typeof(sbyte[])) return StorageClassType.BlobClass;
            if (type == typeof(char)) return StorageClassType.TextClass;
            if (type == typeof(char[])) return StorageClassType.TextClass;
            if (type == typeof(decimal)) return StorageClassType.NumericClass;
            if (type == typeof(double)) return StorageClassType.RealClass;
            if (type == typeof(float)) return StorageClassType.RealClass;
            if (type == typeof(int)) return StorageClassType.IntegerClass;
            if (type == typeof(uint)) return StorageClassType.IntegerClass;
            if (type == typeof(long)) return StorageClassType.IntegerClass;
            if (type == typeof(ulong)) return StorageClassType.IntegerClass;
            if (type == typeof(short)) return StorageClassType.IntegerClass;
            if (type == typeof(ushort)) return StorageClassType.IntegerClass;
            if (type == typeof(string)) return StorageClassType.TextClass;
            if (type == typeof(DateTime)) return StorageClassType.TextClass;
            if (type == typeof(DateTimeOffset)) return StorageClassType.TextClass;
            if (type == typeof(Guid)) return StorageClassType.TextClass;
            if (type == typeof(JObject)) return StorageClassType.TextClass;
            if (type == typeof(JArray)) return StorageClassType.TextClass;
            if (type == typeof(DBNull)) return StorageClassType.IntegerClass;
            throw new ArgumentOutOfRangeException(nameof(type));
        }
    }
}