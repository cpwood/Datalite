using Datalite.Destination;

namespace Datalite.Sources
{
    /// <summary>
    /// Acts as a jumping off point for companion libraries and their extension
    /// methods.
    /// </summary>
    public class AddDataCommand
    {
        /// <summary>
        /// The original Sqlite connection.
        /// </summary>
        public SqliteConnectionBroker Connection { get; }

        internal AddDataCommand(SqliteConnectionBroker connection)
        {
            Connection = connection;
        }
    }
}