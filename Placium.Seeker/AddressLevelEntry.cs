using Newtonsoft.Json;

namespace Placium.Seeker
{
    public class AddressLevelEntry
    {
        [JsonProperty("id")] public string FiasCode { get; set; }

        [JsonProperty("name")] public string Name { get; set; }

        [JsonProperty("type")] public string Type { get; set; }

        [JsonProperty("typeFull")] public string TypeFull { get; set; }
    }
}
