using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using NetTopologySuite.Geometries;
using Npgsql;
using Placium.Common;
using Placium.Models;

namespace Placium.Services
{
    public class PlacexService : BaseApiService
    {
        public PlacexService(IConfiguration configuration) : base(configuration)
        {
        }

        public async Task<List<Placex>> GetByNameAsync(string pattern, string key = "name", int limit = 10)
        {
            using (var connection = new NpgsqlConnection(GetOsmConnectionString()))
            {
                await connection.OpenAsync();

                connection.ReloadTypes();
                connection.TypeMapper.UseNetTopologySuite();

                var result = new List<Placex>(limit);

                using (var command =
                    new NpgsqlCommand(
                        @"SELECT id,tags,location FROM placex
                        WHERE tags->@key SIMILAR TO @pattern
                        LIMIT @limit",
                        connection))
                {
                    command.Parameters.AddWithValue("pattern", pattern);
                    command.Parameters.AddWithValue("key", key);
                    command.Parameters.AddWithValue("limit", limit);

                    command.Prepare();

                    using (var reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                            result.Add(new Placex
                            {
                                id = reader.GetInt64(0),
                                tags = (Dictionary<string, string>) reader.GetValue(1),
                                location = (Geometry) reader.GetValue(2)
                            });
                    }
                }

                await connection.CloseAsync();

                return result;
            }
        }

        public async Task<List<Placex>> GetByCoordsAsync(double latitude, double longitude,
            string key = "addr:housenumber", int limit = 1)
        {
            using (var connection = new NpgsqlConnection(GetOsmConnectionString()))
            {
                await connection.OpenAsync();

                connection.ReloadTypes();
                connection.TypeMapper.UseNetTopologySuite();

                var result = new List<Placex>(limit);

                using (var command =
                    new NpgsqlCommand(
                        @"SELECT id,tags,location FROM placex WHERE tags?@key
                        ORDER BY ST_SetSRID(ST_Point(@longitude,@latitude),4326)<->location
                        LIMIT @limit",
                        connection))
                {
                    command.Parameters.AddWithValue("longitude", (float) longitude);
                    command.Parameters.AddWithValue("latitude", (float) latitude);
                    command.Parameters.AddWithValue("key", key);
                    command.Parameters.AddWithValue("limit", limit);

                    command.Prepare();

                    using (var reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                            result.Add(new Placex
                            {
                                id = reader.GetInt64(0),
                                tags = (Dictionary<string, string>) reader.GetValue(1),
                                location = (Geometry) reader.GetValue(2)
                            });
                    }
                }

                await connection.CloseAsync();

                return result;
            }
        }
    }
}
