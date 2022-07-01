using System;
using System.Collections.Generic;
using System.Linq;

namespace Datalite.Testing
{
    internal static class EqualityExtensionMethods
    {
        public static bool EqualsRecord(this Dictionary<string, object> expected, Dictionary<string, object> actual)
        {
            foreach (var key in expected.Keys)
            {
                var lowerKey = key.ToLowerInvariant();

                // If we're expressing a NULL condition, it doesn't matter if it's missing from the actual dictionary.
                if (expected[key] == DBNull.Value && !actual.ContainsKey(lowerKey))
                    continue;

                // But if we're not expressing a NULL condition, it must exist in the actual dictionary.
                if (!actual.ContainsKey(lowerKey))
                    return false;

                // And does the value type match?
                if (expected[key].GetType() != actual[lowerKey].GetType())
                    return false;

                // And does the value itself match?
                if (expected[key].GetType() == typeof(byte[]))
                {
                    if (!((byte[])expected[key]).SequenceEqual((byte[])actual[lowerKey]))
                        return false;
                }
                else if (!expected[key].Equals(actual[lowerKey]))
                    return false;
            }

            return true;
        }
    }
}
