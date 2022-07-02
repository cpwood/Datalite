using Newtonsoft.Json;

namespace Datalite.Sources.Objects.Tests.Integration
{
    public class TestDataRecord
    {
        [JsonProperty("id")]
        public int? Id { get; set; }

        [JsonProperty("first_name")]
        public string? FirstName { get; set; }

        [JsonProperty("last_name")]
        public string? LastName { get; set; }

        [JsonProperty("email")]
        public string? Email { get; set; }

        [JsonProperty("gender")]
        public string? Gender { get; set; }

        [JsonProperty("image")]
        public byte[]? Image { get; set; }

        [JsonProperty("salary")]
        public decimal? Salary { get; set; }
    }
}
