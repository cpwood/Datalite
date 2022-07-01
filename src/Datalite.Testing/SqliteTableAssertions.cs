using System.Collections.Generic;
using FluentAssertions;
using FluentAssertions.Execution;
using System.Linq;

namespace Datalite.Testing
{
    /// <summary>
    /// Extension methods for SqliteTable Fluent Assertions
    /// </summary>
    public static class SqliteTableAssertionsExtensions
    {
        /// <summary>
        /// Fluent Assertions jump-off point.
        /// </summary>
        /// <returns></returns>
        public static SqliteTableAssertions Should(this SqliteTable? table)
        {
            return new SqliteTableAssertions(table);
        }
    }

    /// <summary>
    /// Assertions for Sqlite tables.
    /// </summary>
    public class SqliteTableAssertions
    {
        /// <summary>
        /// The subject of the tests.
        /// </summary>
        public SqliteTable? Table { get; }

        /// <summary>
        /// Create an instance for the given table.
        /// </summary>
        /// <param name="table">The Sqlite table.</param>
        public SqliteTableAssertions(SqliteTable? table)
        {
            Table = table;
        }

        /// <summary>
        /// The table must exist.
        /// </summary>
        /// <returns></returns>
        public AndConstraint<SqliteTableAssertions> Exist()
        {
            Execute.Assertion
                .ForCondition(Table != null)
                .FailWith("The table was not found.");
            return new AndConstraint<SqliteTableAssertions>(this);
        }

        /// <summary>
        /// The table mustn't exist.
        /// </summary>
        /// <returns></returns>
        public AndConstraint<SqliteTableAssertions> NotExist()
        {
            Execute.Assertion
                .ForCondition(Table == null)
                .FailWith("The table was found.");
            return new AndConstraint<SqliteTableAssertions>(this);
        }

        /// <summary>
        /// The table must have columns that match the provided specification.
        /// </summary>
        /// <param name="columns">The columns specification.</param>
        /// <returns></returns>
        public AndConstraint<SqliteTableAssertions> HaveColumns(params SqliteColumn[] columns)
        {
            foreach (var column in columns)
            {
                this.HaveTheColumn(column);
            }

            return new AndConstraint<SqliteTableAssertions>(this);
        }

        /// <summary>
        /// The table must have a column that matches the provided specification.
        /// </summary>
        /// <param name="column">The column specification.</param>
        /// <returns></returns>
        public AndConstraint<SqliteTableAssertions> HaveTheColumn(SqliteColumn column)
        {
            Execute.Assertion
                .ForCondition(Table?.Columns.Values.Any(x =>
                    x.Name.ToLowerInvariant() == column.Name.ToLowerInvariant() && x.StorageClass == column.StorageClass && x.Required == column.Required) == true)
                .FailWith(
                    $"A column named '{column.Name}' with a Storage Class of '{column.StorageClass}' that is {(!column.Required ? "not " : "")}required could not be found");
            return new AndConstraint<SqliteTableAssertions>(this);
        }

        /// <summary>
        /// The table must have the provided number of columns.
        /// </summary>
        /// <param name="columnCount">The number of columns.</param>
        /// <returns></returns>
        public AndConstraint<SqliteTableAssertions> HaveAColumnCountOf(int columnCount)
        {
            Execute.Assertion
                .ForCondition(Table?.Columns.Count == columnCount)
                .FailWith($"Expected {columnCount} columns but found {Table?.Columns.Count ?? 0}.");
            return new AndConstraint<SqliteTableAssertions>(this);
        }
        
        /// <summary>
        /// The table must have an index covering the provided columns.
        /// </summary>
        /// <param name="columns">The column names.</param>
        /// <returns></returns>
        public AndConstraint<SqliteTableAssertions> HaveAnIndexOf(params string[] columns)
        {
            columns = columns.Select(x => x.ToLowerInvariant()).ToArray();

            Execute.Assertion
                .ForCondition(Table?.Indexes.Any(x => x.SequenceEqual(columns)) == true)
                .FailWith($"An index comprising the columns {string.Join(", ", columns)} could not be found.");
            return new AndConstraint<SqliteTableAssertions>(this);
        }

        /// <summary>
        /// The table must have the provided number of rows.
        /// </summary>
        /// <param name="rowCount">The number of rows.</param>
        /// <returns></returns>
        public AndConstraint<SqliteTableAssertions> HaveARowCountOf(int rowCount)
        {
            Execute.Assertion
                .ForCondition(Table?.Rows.Length == rowCount)
                .FailWith($"Expected {rowCount} rows but found {Table?.Rows.Length ?? 0}.");
            return new AndConstraint<SqliteTableAssertions>(this);
        }

        /// <summary>
        /// The able must have a row that matches the default set of rows.
        /// </summary>
        /// <param name="record"></param>
        /// <returns></returns>
        public AndConstraint<SqliteTableAssertions> HaveARowMatching(Dictionary<string, object> record)
        {
            Execute.Assertion
                .ForCondition(Table?.Rows.Any(record.EqualsRecord) == true)
                .FailWith($"Couldn't find a row matching the specification for the row with an identifier of {record["id"]}");
            return new AndConstraint<SqliteTableAssertions>(this);
        }
    }
}
