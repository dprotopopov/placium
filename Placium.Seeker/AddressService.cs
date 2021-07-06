using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using MySql.Data.MySqlClient;
using Newtonsoft.Json;
using Placium.Common;

namespace Placium.Seeker
{
    public class AddressService : BaseService
    {
        private readonly Regex _spaceRegex = new Regex(@"\s+", RegexOptions.IgnoreCase);

        public AddressService(IConfiguration configuration) : base(configuration)
        {
        }

        public async Task<IEnumerable<AddressEntry>> GetAddressInfoAsync(string searchString, int limit = 20)
        {
            var result = new List<AddressEntry>();
            var list = searchString.Split(",");
            var match = string.Join("<<",
                list.Where(x => !string.IsNullOrWhiteSpace(x)).Select(x =>
                    $"({string.Join(" NEAR/9 ", _spaceRegex.Split(x.Trim()).Select(y => $"\"{y.Yo().ToLower().Escape()}\""))})"));

            using (var mySqlConnection = new MySqlConnection(GetSphinxConnectionString()))
            {
                for (var priority = 0; priority < 20 && limit > 0; priority++)
                {
                    var skip = 0;
                    var take = 20;
                    while (limit > 0)
                    {
                        mySqlConnection.TryOpen();

                        using (var command =
                            new MySqlCommand(
                                @"SELECT addressString,postalCode,regionCode,country,geoLon,geoLat,geoExists FROM address WHERE MATCH(@match) AND priority=@priority LIMIT @skip,@take",
                                mySqlConnection))
                        {
                            command.Parameters.AddWithValue("skip", skip);
                            command.Parameters.AddWithValue("take", take);
                            command.Parameters.AddWithValue("priority", priority);
                            command.Parameters.AddWithValue("match", match);

                            using (var reader = command.ExecuteReader())
                            {
                                var count = 0;
                                while (limit > 0 && reader.Read())
                                {
                                    count++;
                                    limit--;
                                    var addressString = reader.GetString(0);
                                    var postalCode = reader.GetString(1);
                                    var regionCode = reader.GetString(2);
                                    var country = reader.GetString(3);
                                    var geoLon = reader.GetFloat(4);
                                    var geoLat = reader.GetFloat(5);
                                    result.Add(new AddressEntry
                                    {
                                        AddressString = addressString,
                                        PostalCode = postalCode,
                                        RegionCode = regionCode,
                                        Country = country,
                                        GeoLon = JsonConvert.ToString(geoLon),
                                        GeoLat = JsonConvert.ToString(geoLat)
                                    });
                                }

                                if (count < take) break;
                            }
                        }
                    }
                }
            }

            return result;
        }
    }
}