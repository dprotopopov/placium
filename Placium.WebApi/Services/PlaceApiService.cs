using System.Collections.Generic;
using System.Threading.Tasks;
using GeoJSON.Net;
using Microsoft.Extensions.Configuration;
using Npgsql;
using Placium.Common;
using Placium.Seeker;
using Placium.WebApi.Models;

namespace Placium.WebApi.Services
{
    public class PlaceApiService : BaseService
    {
        private readonly DefaultSeeker _seeker;

        public PlaceApiService(IConfiguration configuration, DefaultSeeker seeker) : base(configuration)
        {
            _seeker = seeker;
        }

        public async Task<List<Place>> GetByNameAsync(string pattern, int limit = 10)
        {
            using (var connection = new NpgsqlConnection(GetOsmConnectionString()))
            {
                await connection.OpenAsync();

                connection.ReloadTypes();
                connection.TypeMapper.UseGeoJson();

                var result = new List<Place>(limit);

                using (var command =
                    new NpgsqlCommand(
                        "SELECT id,tags,location FROM place WHERE tags->'name' SIMILAR TO @pattern LIMIT @limit",
                        connection))
                {
                    command.Parameters.AddWithValue("pattern", pattern);
                    command.Parameters.AddWithValue("limit", limit);
                    using (var reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                            result.Add(new Place
                            {
                                id = reader.GetInt64(0),
                                tags = (Dictionary<string, string>) reader.GetValue(1),
                                location = (GeoJSONObject) reader.GetValue(2)
                            });
                    }
                }

                await connection.CloseAsync();

                result.ForEach(async x => { x.guids = await _seeker.AddrToFias(x.tags); });

                return result;
            }
        }

        public async Task<List<Place>> GetByCoordsAsync(double latitude, double longitude, int limit = 1)
        {
            using (var connection = new NpgsqlConnection(GetOsmConnectionString()))
            {
                await connection.OpenAsync();

                connection.ReloadTypes();
                connection.TypeMapper.UseGeoJson();

                var result = new List<Place>(limit);

                using (var command =
                    new NpgsqlCommand(
                        "SELECT id,tags,location FROM place WHERE tags?'addr:housenumber' ORDER BY ST_Distance(location,ST_SetSRID(ST_Point(@longitude,@latitude),4326)::geography) LIMIT @limit",
                        connection))
                {
                    command.Parameters.AddWithValue("longitude", (float) longitude);
                    command.Parameters.AddWithValue("latitude", (float) latitude);
                    command.Parameters.AddWithValue("limit", limit);
                    using (var reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                            result.Add(new Place
                            {
                                id = reader.GetInt64(0),
                                tags = (Dictionary<string, string>) reader.GetValue(1),
                                location = (GeoJSONObject) reader.GetValue(2)
                            });
                    }
                }

                await connection.CloseAsync();

                result.ForEach(async x => { x.guids = await _seeker.AddrToFias(x.tags); });

                return result;
            }
        }
    }
}