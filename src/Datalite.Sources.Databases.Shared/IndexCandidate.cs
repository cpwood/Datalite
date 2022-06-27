namespace Datalite.Sources.Databases.Shared
{
    public class IndexCandidate
    {
        public string? Name { get; set; }
        public string? OriginalType { get; set; }
        public string? ColumnName { get; set; }
        public int? ColumnOrder { get; set; }
    }
}