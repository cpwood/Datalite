using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace Datalite.Testing
{
    /// <summary>
    /// Runs a SQL script to scaffold test data.
    /// </summary>
    public class DataScaffolder
    {
        private readonly DbConnection _connection;

        // Covers standard text and identifier quotes for most database platforms.
        private readonly string _openQuotes = "'[\"`";
        private readonly string _closeQuotes = "']\"`";

        /// <summary>
        /// Create the scaffolder.
        /// </summary>
        /// <param name="connection">Any ADO.NET connection.</param>
        public DataScaffolder(DbConnection connection)
        {
            _connection = connection;
        }

        /// <summary>
        /// Run the provided SQL.
        /// </summary>
        /// <param name="sqlScript">The SQL script.</param>
        /// <returns></returns>
        public async Task ScaffoldAsync(string sqlScript)
        {
            var commands = SplitSql(sqlScript);

            foreach (var command in commands)
            {
                var cmd = _connection.CreateCommand();
                cmd.CommandText = command;
                await cmd.ExecuteNonQueryAsync();
            }
        }

        /// <summary>
        /// Splits the SQL script by the keyword "GO" (SQL Server) or a semi-colon (most
        /// database platforms). Checks that the split keyword isn't quoted.
        /// </summary>
        /// <param name="sql">The full SQL script.</param>
        /// <returns>The individual commands from the SQL script.</returns>
        private string[] SplitSql(string sql)
        {
            // Add some whitespace to the end to make regexes easier
            sql += "\n\n\n";

            // Remove comments
            sql = Regex.Replace(sql, @"--[\s\S]*?$", string.Empty, RegexOptions.Multiline);
            sql = Regex.Replace(sql, @"\/\*[\s\S]*?\*\/", string.Empty);

            var scripts = new List<string>();
            var quoted = false;
            var start = 0;
            var lastUnquoted = 0;
            var quoteTypeIndex = 0;

            for (var i = 0; i < sql.Length; i++)
            {
                if (i < start)
                    continue;

                if (!quoted && _openQuotes.Contains(sql[i]))
                {
                    quoted = true;
                    quoteTypeIndex = _openQuotes.IndexOf(sql[i]);
                }
                else if (quoted && _closeQuotes[quoteTypeIndex] == sql[i])
                {
                    quoted = false;
                    lastUnquoted = i;
                }

                if (!quoted)
                {
                    var fragmentStart = Math.Max(Math.Max(i - 4, lastUnquoted), start);
                    var fragmentLength = i - fragmentStart;
                    var fragment = sql.Substring(fragmentStart, fragmentLength);
                    var match = Regex.Match(fragment, @"\s(GO)\s", RegexOptions.IgnoreCase);
                    var section = string.Empty;

                    var length = -1;

                    if (match.Success)
                    {
                        length = fragmentStart - start;
                        section = sql.Substring(start, length);
                        start = i;
                    }
                    else if (fragment.Contains(";"))
                    {
                        length = (fragmentStart + fragment.IndexOf(";", StringComparison.Ordinal) + 1) - start;
                        section = sql.Substring(start, length);
                        start = start + length;
                    }

                    if (length > -1)
                    {
                        if (!string.IsNullOrEmpty(section.Trim()))
                            scripts.Add(section.Trim());
                    }
                }
            }

            if (!string.IsNullOrEmpty(sql.Substring(start).Trim()))
                scripts.Add(sql.Substring(start).Trim());

            return scripts.ToArray();
        }
    }
}
