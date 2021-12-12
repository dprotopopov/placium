using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Itinero.LocalGeo;
using Microsoft.Extensions.Configuration;
using MySql.Data.MySqlClient;
using Newtonsoft.Json;
using Placium.Common;

namespace Placium.Seeker
{
    public class OsmAddressService : BaseApiService
    {
        public OsmAddressService(IConfiguration configuration) : base(configuration)
        {
        }

        public async Task<IEnumerable<AddressEntry>> GetByCoordsAsync(Coordinate coords, int limit = 20)
        {
            var result = new List<AddressEntry>();
            using (var mySqlConnection = new MySqlConnection(GetSphinxConnectionString()))
            {
                var skip = 0;
                var take = 20;
                while (limit > 0)
                {
                    mySqlConnection.TryOpen();

                    using var command =
                        new MySqlCommand(
                            @"SELECT GEODIST(@lat,@lon,lat,lon) AS distance,title,lon,lat FROM addrx ORDER BY distance ASC LIMIT @skip,@take",
                            mySqlConnection);
                    command.Parameters.AddWithValue("skip", skip);
                    command.Parameters.AddWithValue("take", take);
                    command.Parameters.AddWithValue("lat", coords.Latitude);
                    command.Parameters.AddWithValue("lon", coords.Longitude);

                    using var reader = command.ExecuteReader();
                    var count = 0;
                    while (limit > 0 && reader.Read())
                    {
                        count++;
                        limit--;
                        var addressString = reader.GetString(1);
                        var geoLon = reader.GetFloat(2);
                        var geoLat = reader.GetFloat(3);
                        result.Add(new AddressEntry
                        {
                            AddressString = addressString,
                            GeoLon = JsonConvert.ToString(geoLon),
                            GeoLat = JsonConvert.ToString(geoLat)
                        });
                    }

                    if (count < take) break;
                }
            }

            return result;
        }

        public async Task<IEnumerable<AddressEntry>> GetByNameAsync(string searchString, int limit = 20)
        {
            var result = new List<AddressEntry>();
            var list = searchString.Split(",").ToList();

            var match = list.ToMatch();

            using (var mySqlConnection = new MySqlConnection(GetSphinxConnectionString()))
            {
                var skip = 0;
                var take = 20;
                while (limit > 0)
                {
                    mySqlConnection.TryOpen();

                    using var command =
                        new MySqlCommand(
                            @"SELECT title,lon,lat FROM addrx WHERE MATCH(@match) ORDER BY priority ASC LIMIT @skip,@take",
                            mySqlConnection);
                    command.Parameters.AddWithValue("skip", skip);
                    command.Parameters.AddWithValue("take", take);
                    command.Parameters.AddWithValue("match", match);

                    using var reader = command.ExecuteReader();
                    var count = 0;
                    while (limit > 0 && reader.Read())
                    {
                        count++;
                        limit--;
                        var addressString = reader.GetString(0);
                        var geoLon = reader.GetFloat(1);
                        var geoLat = reader.GetFloat(2);
                        result.Add(new AddressEntry
                        {
                            AddressString = addressString,
                            GeoLon = JsonConvert.ToString(geoLon),
                            GeoLat = JsonConvert.ToString(geoLat)
                        });
                    }

                    if (count < take) break;
                }
            }

            return result;
        }
    }
}