// ReSharper disable ArrangeThisQualifier
// ReSharper disable InconsistentNaming
#pragma warning disable IDE1006 // Naming Styles
using System;
using System.Collections.Generic;
using System.Linq;
using Newtonsoft.Json;

namespace Datalite.Testing
{
    /// <summary>
    /// Represents a record in the test data
    /// </summary>
    public class TestRecord : IRecord
    {
        /// <summary>
        /// The ID.
        /// </summary>
        public int id { get; }

        /// <summary>
        /// First name
        /// </summary>
        public string first_name { get; }

        /// <summary>
        /// Last name
        /// </summary>
        public string last_name { get; }

        /// <summary>
        /// Email
        /// </summary>
        public string? email { get; }

        /// <summary>
        /// Gender
        /// </summary>
        public string gender { get; }

        /// <summary>
        /// Image
        /// </summary>

        public byte[] image_bytes { get; }

        /// <summary>
        /// The Base64 encoded value
        /// </summary>
        public string image_string { get; }

        /// <summary>
        /// Salary
        /// </summary>
        public decimal? salary { get; }

        private readonly Dictionary<string, object> _additionalValues = new Dictionary<string, object>();

        private string _additionalValuesComparer => JsonConvert.SerializeObject(_additionalValues);

        /// <summary>
        /// Creates a test record.
        /// </summary>
        /// <param name="id"></param>
        /// <param name="firstName"></param>
        /// <param name="lastName"></param>
        /// <param name="email"></param>
        /// <param name="gender"></param>
        /// <param name="image">Image as a byte array.</param>
        /// <param name="salary"></param>
        public TestRecord(int id, string firstName, string lastName, string? email, string gender, object image,
            decimal? salary)
        {
            this.id = id;
            this.first_name = firstName;
            this.last_name = lastName;
            this.email = email;
            this.gender = gender;
            this.salary = salary;

            if (image.GetType() == typeof(byte[]))
            {
                image_bytes = (byte[]) image;
                image_string = Convert.ToBase64String(image_bytes);
            }
            else
            {
                image_string = (string) image;
                image_bytes = Convert.FromBase64String(image_string);
            }
        }

        /// <summary>
        /// Add an additional column value to expect.
        /// </summary>
        /// <param name="columnName">The name of the column.</param>
        /// <param name="value">The value.</param>
        public void AddValue(string columnName, object value)
        {
            _additionalValues.Add(columnName.ToLowerInvariant(), value);
        }

        /// <summary>
        /// Equality comparer.
        /// </summary>
        /// <param name="other"></param>
        /// <returns></returns>
        protected bool Equals(TestRecord other)
        {
            return id == other.id &&
                   first_name == other.first_name &&
                   last_name == other.last_name &&
                   email == other.email &&
                   gender == other.gender &&
                   image_bytes.Equals(other.image_bytes) &&
                   salary == other.salary &&
                   _additionalValuesComparer == other._additionalValuesComparer;
        }

        private bool EqualsDictionary(Dictionary<string, object> dictionary)
        {
            try
            {
                if (!dictionary.ContainsKey("id") || (long)dictionary["id"] != this.id)
                    return false;

                if (!dictionary.ContainsKey("first_name") || (string)dictionary["first_name"] != this.first_name)
                    return false;

                if (!dictionary.ContainsKey("last_name") || (string)dictionary["last_name"] != this.last_name)
                    return false;

                if (!string.IsNullOrEmpty(this.email) && !dictionary.ContainsKey("email") ||
                    string.IsNullOrEmpty(this.email) && dictionary.ContainsKey("email") ||
                    !string.IsNullOrEmpty(this.email) && (string)dictionary["email"] != this.email)
                    return false;

                if (!dictionary.ContainsKey("gender") || (string)dictionary["gender"] != this.gender)
                    return false;

                if (!dictionary.ContainsKey("image"))
                    return false;

                if (dictionary["image"].GetType() == typeof(byte[]))
                {
                    if (!((byte[])dictionary["image"]).SequenceEqual(this.image_bytes))
                        return false;
                }
                else if (dictionary["image"] is string @string)
                {
                    if (@string != this.image_string)
                        return false;
                }

                if (this.salary != null && !dictionary.ContainsKey("salary") ||
                    this.salary == null && dictionary.ContainsKey("salary") ||
                    this.salary != null && Math.Abs((double)dictionary["salary"] - (double) this.salary) > 0.01)
                    return false;

                foreach (var key in _additionalValues.Keys)
                {
                    if (!dictionary.ContainsKey(key) || !dictionary[key].Equals(_additionalValues[key]))
                        return false;
                }
            }
            catch (Exception)
            {
                return false;
            }
            
            return true;
        }
        
        /// <summary>
        /// Equality comparer.
        /// </summary>
        /// <param name="obj"></param>
        /// <returns></returns>
        public override bool Equals(object? obj)
        {
            if (obj is null) return false;
            if (ReferenceEquals(this, obj)) return true;

            if (obj.GetType() == typeof(Dictionary<string, object>))
                return EqualsDictionary((Dictionary<string, object>)obj);

            if (obj.GetType() != this.GetType()) return false;
            
            return Equals((TestRecord)obj);
        }

        /// <summary>
        /// Generates a hash code.
        /// </summary>
        /// <returns></returns>
        public override int GetHashCode()
        {
            return HashCode.Combine(id, first_name, last_name, email, gender, image_bytes, salary, _additionalValuesComparer);
        }
    }
}
#pragma warning restore IDE1006 // Naming Styles