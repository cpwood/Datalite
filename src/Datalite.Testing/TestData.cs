using FluentAssertions;

namespace Datalite.Testing
{
    /// <summary>
    /// Helpers for testing the standard test data.
    /// </summary>
    public static class TestData
    {
        /// <summary>
        /// Standard test criteria for the extra table - e.g. "Foo".
        /// </summary>
        /// <param name="tableAssertions"></param>
        /// <param name="columnCount">The column count. Defaults to 2.</param>
        /// <returns></returns>
        public static AndConstraint<SqliteTableAssertions> MeetTheExtraTableConditions(this SqliteTableAssertions tableAssertions, int columnCount = 2)
        {
            var table = tableAssertions.Table;
            return table
                .Should()
                .Exist().And
                .HaveAColumnCountOf(columnCount).And
                .HaveARowCountOf(1);
        }
    }
}
