#pragma warning disable IDE1006 // Naming Styles
namespace Datalite.Testing
{
    /// <summary>
    /// A test record.
    /// </summary>
    public interface IRecord
    {
        // ReSharper disable once InconsistentNaming
        /// <summary>
        /// The identifier.
        /// </summary>
        public int id { get; }
    }
}
#pragma warning restore IDE1006 // Naming Styles