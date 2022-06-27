using System;
using System.Threading.Tasks;
using Datalite.Destination;

namespace Datalite.Sources.Objects
{
    internal class ObjectsDataliteContext : DataliteContext
    {
        public string TableName { get; }
        public object[] Objects { get; }
        public TableDefinition? TableDefinition { get; set; }
        public bool SerializeNested { get; set; } = true;

        public ObjectsDataliteContext(
            string tableName,
            object[] objects,
            Func<ObjectsDataliteContext, Task> executionFunction)
            : base(c => executionFunction((ObjectsDataliteContext)c))
        {
            TableName = tableName;
            Objects = objects;
        }
    }
}
