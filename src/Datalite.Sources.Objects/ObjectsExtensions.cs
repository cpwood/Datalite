using System.IO.Abstractions;

namespace Datalite.Sources.Objects
{
    /// <summary>
    /// .NET CLR Object Extensions
    /// </summary>
    public static class ObjectsExtensions
    {
        /// <summary>
        /// Load data an array of objects into a table with the given name.
        /// </summary>
        /// <param name="adc"></param>
        /// <param name="tableName">The Sqlite table name.</param>
        /// <param name="objects">The object values.</param>
        /// <returns></returns>
        public static ObjectsCommand FromObjects(this AddDataCommand adc, string tableName, object[] objects)
        {
            return adc.FromObjects(tableName, objects, new FileSystem());
        }

        /// <summary>
        /// Load data an array of objects into a table with the given name.
        /// </summary>
        /// <param name="adc"></param>
        /// <param name="tableName">The Sqlite table name.</param>
        /// <param name="objects">The object values.</param>
        /// <param name="fileSystem">The filesystem to use. This could be something other than the local file system.</param>
        /// <returns></returns>
        public static ObjectsCommand FromObjects(this AddDataCommand adc, string tableName, object[] objects, IFileSystem fileSystem)
        {
            var service = new ObjectsService(adc.Connection, fileSystem);
            var context = new ObjectsDataliteContext(tableName, objects, ctx => service.ExecuteAsync(ctx));
            return new ObjectsCommand(context);
        }
    }
}