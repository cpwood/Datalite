using System;
using System.Collections;
using System.Collections.Generic;

namespace Datalite.Exceptions
{
    /// <summary>
    /// Describes an exception that has happened within a Datalite library.
    /// </summary>
    public class DataliteException : Exception
    {
        /// <summary>
        /// Creates a new <see cref="DataliteException"/> with the provided <paramref name="message"/>/
        /// </summary>
        /// <param name="message">The exception message</param>
        public DataliteException(string message) : base(message)
        {
            Data = new Dictionary<string, object>();
        }

        /// <summary>
        /// Creates a new <see cref="DataliteException"/> with the provided <paramref name="message"/>/
        /// </summary>
        /// <param name="message">The exception message</param>
        /// <param name="data">A dictionary of values to include in the exception.</param>
        public DataliteException(string message, Dictionary<string, object> data) : base(message)
        {
            Data = data;
        }

        public override IDictionary Data { get; }
    }
}