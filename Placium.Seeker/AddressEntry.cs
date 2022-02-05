using Newtonsoft.Json;

namespace Placium.Seeker;

public class AddressEntry
{
    [JsonProperty("address")] public string AddressString { get; set; }

    [JsonProperty("postalCode")] public string PostalCode { get; set; }

    [JsonProperty("regionCode")] public string RegionCode { get; set; }

    [JsonProperty("country")] public string Country { get; set; }

    [JsonProperty("region")] public AddressLevelEntry Region { get; set; } = new();

    [JsonProperty("area")] public AddressLevelEntry Area { get; set; } = new();

    [JsonProperty("city")] public AddressLevelEntry City { get; set; } = new();

    [JsonProperty("cityDistrict")] public AddressLevelEntry CityDistrict { get; set; } = new();

    [JsonProperty("settlement")] public AddressLevelEntry Settlement { get; set; } = new();

    [JsonProperty("street")] public AddressLevelEntry Street { get; set; } = new();

    [JsonProperty("house")] public AddressLevelEntry House { get; set; } = new();

    [JsonProperty("block")] public AddressLevelEntry HouseBlock { get; set; } = new();

    [JsonProperty("flat")] public AddressLevelEntry Flat { get; set; } = new();

    [JsonProperty("geolat")] public string GeoLat { get; set; }

    [JsonProperty("geolon")] public string GeoLon { get; set; }
}