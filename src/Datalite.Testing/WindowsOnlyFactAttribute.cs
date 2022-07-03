using System;
using Xunit;

namespace Datalite.Testing
{
    /// <summary>
    /// A test that will only run on Windows.
    /// </summary>
    public class WindowsOnlyFactAttribute : FactAttribute
    {
        /// <inheritdoc />
        public WindowsOnlyFactAttribute()
        {
            if (Environment.OSVersion.Platform != PlatformID.Win32NT)
                Skip = "This test is only executed on Windows.";
        }

        /// <inheritdoc />
        public sealed override string? Skip { get; set; }
    }
}
