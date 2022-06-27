using System;
using Newtonsoft.Json.Linq;

namespace Datalite.Destination
{
    public static class StoragesClasses
    {
        /// <summary>
        /// The Sqlite Storage Classes
        /// </summary>
        public enum StorageClassType
        {
            IntegerClass,
            RealClass,
            TextClass,
            BlobClass,
            NumericClass
        }

        /// <summary>
        /// Gets the DDL representation of a <see cref="StorageClassType"/> value.
        /// </summary>
        /// <param name="type">The <see cref="StorageClassType"/> instance.</param>
        /// <returns></returns>
        public static string AsString(this StorageClassType type)
        {
            switch (type)
            {
                case StorageClassType.IntegerClass:
                    return "INTEGER";
                case StorageClassType.RealClass:
                    return "REAL";
                case StorageClassType.TextClass:
                    return "TEXT";
                case StorageClassType.BlobClass:
                    return "BLOB";
                case StorageClassType.NumericClass:
                    return "NUMERIC";
            }

            return "TEXT";
        }

        /// <summary>
        /// Gets the <see cref="StorageClassType"/> representation of the DDL storage class sname.
        /// </summary>
        /// <param name="storageClassName">The DDL storage class name.</param>
        /// <returns></returns>
        public static StorageClassType GetStorageClassTypeFromName(string storageClassName)
        {
            switch (storageClassName)
            {
                case "INTEGER":
                    return StorageClassType.IntegerClass;
                case "REAL":
                    return StorageClassType.RealClass;
                case "TEXT":
                    return StorageClassType.TextClass;
                case "BLOB":
                    return StorageClassType.BlobClass;
                case "NUMERIC":
                    return StorageClassType.NumericClass;
            }

            return StorageClassType.TextClass;
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