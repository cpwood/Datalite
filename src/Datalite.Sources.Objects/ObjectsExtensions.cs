using System.IO.Abstractions;

namespace Datalite.Sources.Objects
{
    public static class ObjectsExtensions
    {
        public static ObjectsCommand FromObjects(this AddDataCommand adc, string tableName, object[] objects)
        {
            return adc.FromObjects(tableName, objects, new FileSystem());
        }

        public static ObjectsCommand FromObjects(this AddDataCommand adc, string tableName, object[] objects, IFileSystem fileSystem)
        {
            var service = new ObjectsService(adc.Connection, fileSystem);
            var context = new ObjectsDataliteContext(tableName, objects, ctx => service.ExecuteAsync(ctx));
            return new ObjectsCommand(context);
        }
    }
}