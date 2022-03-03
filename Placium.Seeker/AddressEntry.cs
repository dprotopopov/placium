using Newtonsoft.Json;

namespace Placium.Seeker
{
    public class AddressEntry
    {
        [JsonProperty("address")] public string AddressString { get; set; }

        [JsonProperty("postalCode")] public string PostalCode { get; set; }

        [JsonProperty("regionCode")] public string RegionCode { get; set; }

        [JsonProperty("country")] public string Country { get; set; }

        [JsonProperty("region")] public AddressLevelEntry Region { get; set; } = new AddressLevelEntry();

        [JsonProperty("area")] public AddressLevelEntry Area { get; set; } = new AddressLevelEntry();

        [JsonProperty("city")] public AddressLevelEntry City { get; set; } = new AddressLevelEntry();

        [JsonProperty("cityDistrict")] public AddressLevelEntry CityDistrict { get; set; } = new AddressLevelEntry();

        [JsonProperty("settlement")] public AddressLevelEntry Settlement { get; set; } = new AddressLevelEntry();

        [JsonProperty("street")] public AddressLevelEntry Street { get; set; } = new AddressLevelEntry();

        [JsonProperty("house")] public AddressLevelEntry House { get; set; } = new AddressLevelEntry();

        [JsonProperty("block")] public AddressLevelEntry HouseBlock { get; set; } = new AddressLevelEntry();

        [JsonProperty("flat")] public AddressLevelEntry Flat { get; set; } = new AddressLevelEntry();

        [JsonProperty("geolat")] public string GeoLat { get; set; }

        [JsonProperty("geolon")] public string GeoLon { get; set; }
    }
}