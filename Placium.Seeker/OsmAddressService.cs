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
    public class OsmAddressService : BaseApiService
    {
        private readonly ILogger _logger;

        public OsmAddressService(IConfiguration configuration, ILogger<OsmAddressService> logger) : base(configuration)
        {
            _logger = logger;
        }

        public async Task<IEnumerable<AddressEntry>> GetByCoordsAsync(Coordinate coords, int limit = 20,
            bool raw = false, bool custom = false, string filter = null)
        {
            try
            {
                if (!coords.Valid) throw new ArgumentException(nameof(coords));

                var level = custom ? 1 : 0;

                var andFilter = !string.IsNullOrWhiteSpace(filter)
                    ? $"AND ({filter})" : "";

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
            catch (Exception ex)
            {
                _logger.LogError(ex.Message);
                throw;
            }
        }

        public async Task<IEnumerable<AddressEntry>> GetByNameAsync(string searchString, int limit = 20,
            bool raw = false, bool custom = false, string filter = null)
        {
            try
            {
                var result = new List<AddressEntry>();

                if (string.IsNullOrEmpty(searchString)) return result;

                var title = custom ? "custom_title" : "title";
                var level = custom ? 1 : 0;

                var andFilter = !string.IsNullOrWhiteSpace(filter)
                    ?$"AND ({filter})" : "";

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
                        $@"SELECT title,lon,lat,data FROM addrx WHERE MATCH(@match) AND custom_level>=@level {andFilter} ORDER BY priority ASC,title ASC LIMIT @skip,@take",
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
            catch (Exception ex)
            {
                _logger.LogError(ex.Message);
                throw;
            }
        }
    }
}