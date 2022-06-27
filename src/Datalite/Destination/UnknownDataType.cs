using System;

namespace Datalite.Destination
{
    /// <summary>
    /// Used to denote that the data type for a column is not known
    /// at the current time.
    ///
    /// This class cannot be instantiated.
    /// </summary>
    public sealed class UnknownDataType
    {
        internal UnknownDataType()
        {
            throw new NotSupportedException();
        }
    }
}
