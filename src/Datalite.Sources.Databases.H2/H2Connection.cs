namespace Datalite.Sources.Databases.H2
{
    /// <summary>
    /// Stores H2 database connection information.
    /// </summary>
    public class H2Connection
    {
        /// <summary>
        /// Versions of the H2 database format.
        /// </summary>
        public enum H2Version
        {
            /// <summary>
            /// Version 1 of the H2 database format.
            /// </summary>
            Version1,

            /// <summary>
            /// Version 2 of the H2 database format.
            /// </summary>
            Version2
        };

        /// <summary>
        /// The JDBC connection string - e.g. "jdbc:h2:./database-name".
        /// </summary>
        public string ConnectionString { get; }

        /// <summary>
        /// The database username.
        /// </summary>
        public string Username { get; }

        /// <summary>
        /// The database password.
        /// </summary>
        public string Password { get; }

        /// <summary>
        /// The version of the H2 driver to use. Defaults to Version 2.
        /// </summary>
        public H2Version Version { get; }

        /// <summary>
        /// Stores the information required to connect to an H2 database via JDBC.
        /// </summary>
        /// <param name="connectionString">The JDBC connection string - e.g. "jdbc:h2:./database-name".</param>
        /// <param name="username">The database username.</param>
        /// <param name="password">The database password.</param>
        /// <param name="version">The version of the H2 driver to use. Defaults to Version 2.</param>
        public H2Connection(string connectionString,
            string username,
            string password,
            H2Version version = H2Version.Version2)
        {
            ConnectionString = connectionString;
            Username = username;
            Password = password;
            Version = version;
        }
    }
}
