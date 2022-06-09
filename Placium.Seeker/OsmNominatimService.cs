using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using MySql.Data.MySqlClient;
using Newtonsoft.Json;
using Placium.Common;
using Route.LocalGeo;

namespace Placium.Seeker
{
    public class OsmNominatimService : BaseApiService
    {
        private readonly ILogger _logger;

        public OsmNominatimService(IConfiguration configuration, ILogger<OsmNominatimService> logger) : base(
            configuration)
        {
            _logger = logger;
        }

        public async Task<IEnumerable<NominatimEntry>> GetByCoordsAsync(float lat, float lon, int limit = 20,
            bool raw = false, bool custom = false)
        {
            if (!Coordinate.Validate(lat, lon)) throw new ArgumentException($"{nameof(lat)},{nameof(lon)}");

            var level = custom ? 1 : 0;

            var result = new List<NominatimEntry>();
            await using var mySqlConnection = new MySqlConnection(GetSphinxConnectionString());
            var skip = 0;
            var take = 20;
            while (limit > 0)
            {
                mySqlConnection.TryOpen();

                await using var command =
                    new MySqlCommand(
                        @"SELECT GEODIST(@lat,@lon,lat,lon,{in=degrees,out=meters}) AS distance,title,lon,lat,data FROM addrx WHERE custom_level>=@level ORDER BY distance ASC LIMIT @skip,@take",
                        mySqlConnection);
                command.Parameters.AddWithValue("skip", skip);
                command.Parameters.AddWithValue("take", take);
                command.Parameters.AddWithValue("lat", lat);
                command.Parameters.AddWithValue("lon", lon);

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

                        result.Add(new NominatimEntry
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

        public async Task<IEnumerable<NominatimEntry>> GetByNameAsync(string searchString, int limit = 20,
            bool raw = false, bool custom = false)
        {
            var result = new List<NominatimEntry>();

            if (string.IsNullOrEmpty(searchString)) return result;

            var title = custom ? "custom_title" : "title";
            var sorting = custom ? "custom_sorting" : "sorting";
            var level = custom ? 1 : 0;

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
                        $@"SELECT title,lon,lat,data FROM addrx WHERE MATCH(@match) AND custom_level>=@level ORDER BY priority ASC,{sorting} ASC LIMIT @skip,@take",
                        mySqlConnection);
                command.Parameters.AddWithValue("skip", skip);
                command.Parameters.AddWithValue("take", take);
                command.Parameters.AddWithValue("match", match);
                command.Parameters.AddWithValue("level", level);

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

                        result.Add(new NominatimEntry
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