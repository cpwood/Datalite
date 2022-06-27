using Datalite.Destination;

namespace Datalite.Testing
{
    public class SqliteColumn
    {
        public string Name { get; }
        public StoragesClasses.StorageClassType StorageClass { get; }
        public bool Required { get; }

        public SqliteColumn(string name, StoragesClasses.StorageClassType storageClass, bool required)
        {
            Name = name;
            StorageClass = storageClass;
            Required = required;
        }
    }
}
