using System;

namespace Datalite.Sources
{
    /// <summary>
    /// <para>
    /// Explains how to deal with source string data that might be ambiguous for
    /// the destination storage class.
    /// </para>
    /// <para>
    /// For example, if text is to be changed into a Blob value, is the text Base64 or a hex
    /// string? If a number has alphanumerics within it (e.g. £ or $), should it
    /// be stripped first?
    /// </para>
    /// <para>
    /// Values can be combined:
    /// </para>
    /// <code>var flags = StringValueInterpretation.EmptyStringIsNull | StringValueInterpretation.StripAlpha;</code>
    /// </summary>
    [Flags]
    public enum StringValueInterpretation
    {
        /// <summary>
        /// Literal interpretation of the string value with no preprocessing.
        /// </summary>
        Default = 0,

        /// <summary>
        /// Treat an empty string as a null value.
        /// </summary>
        EmptyStringIsNull = 1,

        /// <summary>
        /// Treat the literal string value "null" as a null value.
        /// </summary>
        LiteralNullIsNull = 2,

        /// <summary>
        /// The string is binary represented as Base64.
        /// </summary>
        Base64 = 4,

        /// <summary>
        /// The string is binary represented as hexadecimal values.
        /// </summary>
        Hex = 8,

        /// <summary>
        /// Alphabetical characters must be stripped before parsing the string as a number.
        /// This excludes the decimal separator for the current culture and the negative symbol.
        /// </summary>
        StripAlpha = 16,

        /// <summary>
        /// The source date is in a local format for the current culture and should be converted
        /// to ISO 8601 format for storage.
        /// </summary>
        LocalDate = 32
    }
}
