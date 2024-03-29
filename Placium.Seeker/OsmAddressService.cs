﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using MySql.Data.MySqlClient;
using Newtonsoft.Json;
using Placium.Common;
using Route.LocalGeo;

namespace Placium.Seeker
{
    public class OsmAddressService : BaseApiService
    {
        private readonly ILogger _logger;

        public OsmAddressService(IConfiguration configuration, ILogger<OsmAddressService> logger) : base(configuration)
        {
            _logger = logger;
        }

        private static bool TryDeserializeObject<T>(string str, out T obj)
        {
            try
            {
                obj = JsonConvert.DeserializeObject<T>(str);
                return true;
            }
            catch
            {
                obj = default;
                return false;
            }
        }

        public async Task<IEnumerable<AddressEntry>> GetByCoordsAsync(Coordinate coords, int limit = 20,
            bool raw = false, bool custom = false, string filter = null)
        {
            if (!coords.Valid) throw new ArgumentException(nameof(coords));

            var level = custom ? 1 : 0;

            var deserialized = !string.IsNullOrWhiteSpace(filter)
                ? TryDeserializeObject(filter, out List<Dictionary<string, string>> obj) ? obj
                : TryDeserializeObject($"[{filter}]", out List<Dictionary<string, string>> obj1) ? obj1
                : null
                : null;

            var sanitized = deserialized?.Select(dictionary => dictionary
                .Where(x => Regex.IsMatch(x.Key, @"^[A-Za-z0-9\.\:_\`]+$"))
                .ToDictionary(x => x.Key, x => x.Value)).Where(item => item.Any()).ToList();

            var andFilter = sanitized != null && sanitized.Any()
                ? " AND " + string.Join(" OR ",
                    sanitized.Select((item, index) =>
                        $"( {string.Join(" AND ", item.Select(x => $"data.`{x.Key}`=@data_{x.Key.Replace(":", "_")}_{index}"))} )"))
                : "";

            var result = new List<AddressEntry>();
            await using var mySqlConnection = new MySqlConnection(GetSphinxConnectionString());
            var skip = 0;
            var take = 20;
            while (limit > 0)
            {
                mySqlConnection.TryOpen();

                await using var command =
                    new MySqlCommand(
                        $@"SELECT GEODIST(@lat,@lon,lat,lon,{{in=degrees,out=meters}}) AS distance,title,lon,lat,data FROM addrx WHERE custom_level>=@level {andFilter} ORDER BY distance ASC LIMIT @skip,@take",
                        mySqlConnection);
                command.Parameters.AddWithValue("skip", skip);
                command.Parameters.AddWithValue("take", take);
                command.Parameters.AddWithValue("lat", coords.Latitude);
                command.Parameters.AddWithValue("lon", coords.Longitude);
                command.Parameters.AddWithValue("level", level);

                if (sanitized != null)
                {
                    var index = 0;
                    foreach (var dictionary in sanitized)
                    {
                        foreach (var (key, value) in dictionary)
                            command.Parameters.AddWithValue($"data_{key.Replace(":", "_")}_{index}", value);
                        index++;
                    }
                }

                await using var reader = command.ExecuteReader();
                var count = 0;
                while (limit > 0 && reader.Read())
                {
                    count++;
                    var addressString = reader.GetString(1);
                    var geoLon = reader.GetFloat(2);
                    var geoLat = reader.GetFloat(3);
                    var data = reader.GetString(4);
                    if (raw || result.All(x =>
                            string.Compare(x.AddressString, addressString,
                                StringComparison.InvariantCultureIgnoreCase) != 0))
                    {
                        limit--;

                        result.Add(new AddressEntry
                        {
                            AddressString = addressString,
                            GeoLon = JsonConvert.ToString(geoLon),
                            GeoLat = JsonConvert.ToString(geoLat),
                            Data = JsonConvert.DeserializeObject<Dictionary<string, string>>(data)
                        });
                    }
                }

                if (count < take) break;

                skip += take;
            }

            return result;
        }

        public async Task<IEnumerable<AddressEntry>> GetByNameAsync(string searchString, int limit = 20,
            bool raw = false, bool custom = false, string filter = null)
        {
            var result = new List<AddressEntry>();

            if (string.IsNullOrEmpty(searchString)) return result;

            var title = custom ? "custom_title" : "title";
            var sorting = custom ? "custom_sorting" : "sorting";
            var level = custom ? 1 : 0;

            var deserialized = !string.IsNullOrWhiteSpace(filter)
                ? TryDeserializeObject(filter, out List<Dictionary<string, string>> obj) ? obj
                : TryDeserializeObject($"[{filter}]", out List<Dictionary<string, string>> obj1) ? obj1
                : null
                : null;

            var sanitized = deserialized?.Select(dictionary => dictionary
                .Where(x => Regex.IsMatch(x.Key, @"^[A-Za-z0-9\.\:_\`]+$"))
                .ToDictionary(x => x.Key, x => x.Value)).Where(item => item.Any()).ToList();

            var andFilter = sanitized != null && sanitized.Any()
                ? " AND " + string.Join(" OR ",
                    sanitized.Select((item, index) =>
                        $"( {string.Join(" AND ", item.Select(x => $"data.`{x.Key}`=@data_{x.Key.Replace(":", "_")}_{index}"))} )"))
                : "";

            var list = searchString.Split(",").ToList();

            var match = $"@({title}) ({list.ToMatch()})";

            await using var mySqlConnection = new MySqlConnection(GetSphinxConnectionString());
            var skip = 0;
            var take = 20;

            while (limit > 0)
            {
                mySqlConnection.TryOpen();

                await using var command =
                    new MySqlCommand(
                        $@"SELECT title,lon,lat,data FROM addrx WHERE MATCH(@match) AND custom_level>=@level {andFilter} ORDER BY priority ASC,{sorting} ASC LIMIT @skip,@take",
                        mySqlConnection);
                command.Parameters.AddWithValue("skip", skip);
                command.Parameters.AddWithValue("take", take);
                command.Parameters.AddWithValue("match", match);
                command.Parameters.AddWithValue("level", level);

                if (sanitized != null)
                {
                    var index = 0;
                    foreach (var dictionary in sanitized)
                    {
                        foreach (var (key, value) in dictionary)
                            command.Parameters.AddWithValue($"data_{key.Replace(":", "_")}_{index}", value);
                        index++;
                    }
                }

                await using var reader = command.ExecuteReader();
                var count = 0;

                while (limit > 0 && reader.Read())
                {
                    count++;
                    var addressString = reader.GetString(0);
                    var geoLon = reader.GetFloat(1);
                    var geoLat = reader.GetFloat(2);
                    var data = reader.GetString(3);
                    if (raw || result.All(x =>
                            string.Compare(x.AddressString, addressString,
                                StringComparison.InvariantCultureIgnoreCase) != 0))
                    {
                        limit--;

                        result.Add(new AddressEntry
                        {
                            AddressString = addressString,
                            GeoLon = JsonConvert.ToString(geoLon),
                            GeoLat = JsonConvert.ToString(geoLat),
                            Data = JsonConvert.DeserializeObject<Dictionary<string, string>>(data)
                        });
                    }
                }

                if (count < take) break;

                skip += take;
            }

            return result;
        }
    }
}