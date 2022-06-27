namespace Datalite.Sources.Databases.H2
{
    internal class QueryResult
    {
        public string SchemaFilename { get; }
        public string DataFilename { get; }

        public QueryResult(string schemaFilename, string dataFilename)
        {
            SchemaFilename = schemaFilename;
            DataFilename = dataFilename;
        }
    }
}
