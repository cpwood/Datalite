using System;
using Datalite.Destination;
using Newtonsoft.Json;

namespace Datalite.Sources.Files.Json
{
    internal static class JsonReaderExtensions
    {
        /// <summary>
        /// Read the value token from the JSON property according to its JSON
        /// token type, convert it to the Sqlite target storage class and encode
        /// it so it can be dropped straight into a T-SQL "INSERT" statement.
        /// </summary>
        /// <param name="reader">The JsonReader.</param>
        /// <param name="column">The column that will contain the read value.</param>
        /// <returns></returns>
        public static string GetSqlValue(this JsonReader reader, Column column)
        {
            if (reader.Value == null) return "NULL";

            return reader.TokenType switch
            {
                JsonToken.Boolean => ((bool)reader.Value).As(column.StorageClass),
                JsonToken.Bytes => ((byte[])reader.Value).As(column.StorageClass),
                JsonToken.Date => ((DateTime)reader.Value).As(column.StorageClass),
                JsonToken.Null => "NULL",
                JsonToken.Integer => ((long)reader.Value).As(column.StorageClass),
                JsonToken.Float => ((double)reader.Value).As(column.StorageClass),
                _ => reader.Value != null ? (reader.Value.ToString() ?? string.Empty).As(column.StorageClass) : "NULL"
            };
        }
    }
}