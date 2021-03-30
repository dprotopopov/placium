﻿using System.Collections.Generic;
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

        public async Task<List<Place>> GetByNameAsync(string pattern, int limit = 10)
        {
            using (var connection = new NpgsqlConnection(GetConnectionString()))
            {
                await connection.OpenAsync();

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

                return result;
            }
        }

        public async Task<List<Place>> GetByPointAsync(double latitude, double longitude, int limit = 1)
        {
            using (var connection = new NpgsqlConnection(GetConnectionString()))
            {
                await connection.OpenAsync();

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

                return result;
            }
        }

        private string GetConnectionString()
        {
            return _configuration.GetConnectionString("OsmConnection");
        }
    }
}