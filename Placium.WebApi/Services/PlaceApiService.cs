using System.Collections.Generic;
using System.Threading.Tasks;
using GeoJSON.Net;
using Microsoft.Extensions.Configuration;
using Npgsql;
using Placium.WebApi.Models;

namespace Placium.WebApi.Services
{
    public class PlaceApiService
    {
        private readonly IConfiguration _configuration;

        public PlaceApiService(IConfiguration configuration)
        {
            _configuration = configuration;
        }

        public async Task<List<Place>> GetByNameAsync(string pattern, int limit = 100)
        {
            using (var connection = new NpgsqlConnection(GetConnectionString()))
            {
                connection.Open();
                connection.TypeMapper.UseGeoJson();
                var result = new List<Place>(limit);

                using (var command =
                    new NpgsqlCommand(
                        "SELECT tags->'name',tags,location FROM place WHERE tags->'name' SIMILAR TO @pattern LIMIT @limit",
                        connection))
                {
                    command.Parameters.AddWithValue("pattern", pattern);
                    command.Parameters.AddWithValue("limit", limit);
                    using (var reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                            result.Add(new Place
                            {
                                title = reader.GetString(0),
                                tags = (Dictionary<string, string>) reader.GetValue(1),
                                location = (GeoJSONObject) reader.GetValue(2)
                            });
                    }
                }

                return result;
            }
        }

        private string GetConnectionString()
        {
            return _configuration.GetConnectionString("OsmConnection");
        }
    }
}