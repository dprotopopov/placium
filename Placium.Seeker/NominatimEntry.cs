using Newtonsoft.Json;

namespace Placium.Seeker
{
    public class NominatimEntry
    {
        [JsonProperty("display_name")] public string AddressString { get; set; }
        [JsonProperty("lat")] public string GeoLat { get; set; }

        [JsonProperty("lon")] public string GeoLon { get; set; }
    }
}