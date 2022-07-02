using Newtonsoft.Json;

namespace Datalite.Sources.Objects.Tests.Integration
{
    public class SubRecord
    {
        [JsonProperty("foo")]
        public string Foo { get; set; } = "Bar";
    }
}
