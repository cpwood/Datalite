using System.Linq;
using Datalite.Destination;
using FluentAssertions;
using FluentAssertions.Execution;

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
        private readonly SqliteTable? _table;

        /// <summary>
        /// Create an instance for the given table.
        /// </summary>
        /// <param name="table">The Sqlite table.</param>
        public SqliteTableAssertions(SqliteTable? table)
        {
            _table = table;
        }

        /// <summary>
        /// The table must exist.
        /// </summary>
        /// <returns></returns>
        public AndConstraint<SqliteTableAssertions> Exist()
        {
            Execute.Assertion
                .ForCondition(_table != null)
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
                .ForCondition(_table == null)
                .FailWith("The table was found.");
            return new AndConstraint<SqliteTableAssertions>(this);
        }

        /// <summary>
        /// The table must have a column that matches the provided specification.
        /// </summary>
        /// <param name="column">The column specification.</param>
        /// <returns></returns>
        public AndConstraint<SqliteTableAssertions> HaveColumn(SqliteColumn column)
        {
            Execute.Assertion
                .ForCondition(_table?.Columns.Values.Any(x =>
                    x.Name == column.Name && x.StorageClass == column.StorageClass && x.Required == column.Required) == true)
                .FailWith(
                    $"A column named '{column.Name}' with a Storage Class of '{column.StorageClass}' that is {(!column.Required ? "not " : "")}required could not be found");
            return new AndConstraint<SqliteTableAssertions>(this);
        }

        /// <summary>
        /// The table must have a column that matches one of the provided specifications.
        /// </summary>
        /// <param name="optionA">A column specification.</param>
        /// <param name="optionB">A column specification.</param>
        /// <returns></returns>
        public AndConstraint<SqliteTableAssertions> HaveColumn(SqliteColumn optionA, SqliteColumn optionB)
        {
            Execute.Assertion
                .ForCondition(_table?.Columns.Values.Any(x =>
                    (x.Name == optionA.Name && x.StorageClass == optionA.StorageClass && x.Required == optionA.Required) ||
                              (x.Name == optionB.Name && x.StorageClass == optionB.StorageClass && x.Required == optionB.Required)) == true)
                .FailWith(
                    $"A column named '{optionA.Name}' with a Storage Class of '{optionA.StorageClass}' that is {(!optionA.Required ? "not " : "")}required could not be found." +
                    $"Additionally, a column named '{optionB.Name}' with a Storage Class of '{optionB.StorageClass}' that is {(!optionB.Required ? "not " : "")}required could not be found.");
            return new AndConstraint<SqliteTableAssertions>(this);
        }

        /// <summary>
        /// The table must have the provided number of columns.
        /// </summary>
        /// <param name="columnCount">The number of columns.</param>
        /// <returns></returns>
        public AndConstraint<SqliteTableAssertions> HaveColumnCount(int columnCount)
        {
            Execute.Assertion
                .ForCondition(_table?.Columns.Count == columnCount)
                .FailWith($"Expected {columnCount} columns but found {_table?.Columns.Count ?? 0}.");
            return new AndConstraint<SqliteTableAssertions>(this);
        }
        
        /// <summary>
        /// The table must have an index covering the provided columns.
        /// </summary>
        /// <param name="columns">The column names.</param>
        /// <returns></returns>
        public AndConstraint<SqliteTableAssertions> HaveIndex(params string[] columns)
        {
            Execute.Assertion
                .ForCondition(_table?.Indexes.Any(x => x.SequenceEqual(columns)) == true)
                .FailWith($"An index comprising the columns {string.Join(", ", columns)} could not be found.");
            return new AndConstraint<SqliteTableAssertions>(this);
        }

        /// <summary>
        /// The table must have the provided number of rows.
        /// </summary>
        /// <param name="rowCount">The number of rows.</param>
        /// <returns></returns>
        public AndConstraint<SqliteTableAssertions> HaveRowCount(int rowCount)
        {
            Execute.Assertion
                .ForCondition(_table?.Rows.Length == rowCount)
                .FailWith($"Expected {rowCount} rows but found {_table?.Rows.Length ?? 0}.");
            return new AndConstraint<SqliteTableAssertions>(this);
        }

        /// <summary>
        /// The table must have a row that matches the provided sequence of columnar values.
        /// </summary>
        /// <param name="columnValues">The columnar values.</param>
        /// <returns></returns>
        public AndConstraint<SqliteTableAssertions> HaveRowMatching(params object[] columnValues)
        {
            var stringified = columnValues.Select(x => x.Convert(x.GetType(), StoragesClasses.FromType(x.GetType())))
                .ToArray();

            Execute.Assertion
                .ForCondition(_table?.Rows.Any(x => x.SequenceEqual(stringified)) == true)
                .FailWith($"Couldn't find a row with the column values provided: {string.Join(',', stringified)}");
            return new AndConstraint<SqliteTableAssertions>(this);
        }
    }
}
