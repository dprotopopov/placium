using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using NetTopologySuite.Geometries;
using Npgsql;
using Placium.Common;
using Placium.Models;
using Coordinate = Route.LocalGeo.Coordinate;

namespace Placium.Services
{
    public class SvgService : BaseApiService
    {
        private readonly GeometryFactory _geometryFactory = new GeometryFactory();
        private readonly ILogger _logger;

        public SvgService(IConfiguration configuration, ILogger<SvgService> logger) : base(configuration)
        {
            _logger = logger;
        }

        public async Task<string> GetSvg(List<string> keys, int width, int height)
        {
            await using var connection = new NpgsqlConnection(GetOsmConnectionString());
            await connection.OpenAsync();

            connection.ReloadTypes();
            connection.TypeMapper.UseNetTopologySuite();

            var types = keys.Select(x =>
                x.StartsWith("R", StringComparison.InvariantCultureIgnoreCase) ? "relation" :
                x.StartsWith("W", StringComparison.InvariantCultureIgnoreCase) ? "way" :
                x.StartsWith("N", StringComparison.InvariantCultureIgnoreCase) ? "node" :
                throw new ArgumentException()).ToArray();
            var ids = keys.Select(x => long.Parse(x.Substring(1))).ToArray();

            var result = new List<Placex>(keys.Count);

            try
            {
                await using (var command =
                             new NpgsqlCommand(
                                 @"SELECT id,tags,ST_ShiftLongitude(location)
                             FROM (SELECT p.id,p.tags,p.location FROM placex p
                             JOIN (SELECT unnest(@types::osm_type[]) AS type, unnest(@ids) AS id) t
                             ON p.osm_type=t.type AND p.osm_id=t.id) q",
                                 connection))
                {
                    command.Parameters.AddWithValue("types", types);
                    command.Parameters.AddWithValue("ids", ids);

                    await command.PrepareAsync();

                    await using var reader = command.ExecuteReader();
                    while (reader.Read())
                        result.Add(new Placex
                        {
                            id = reader.GetInt64(0),
                            tags = (Dictionary<string, string>)reader.GetValue(1),
                            location = (Geometry)reader.GetValue(2)
                        });
                }

                await connection.CloseAsync();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, ex.Message);
            }

            var g = _geometryFactory.CreateEmpty(Dimension.Surface);
            foreach (var item in result)
            {
                var g1 = item.location;
                item.location = g1 = g1.Buffer(0);
                g = g.Union(g1);
            }

            var envelope = g.EnvelopeInternal;
            var centerX = (envelope.MaxX + envelope.MinX) / 2.0;
            var centerY = (envelope.MaxY + envelope.MinY) / 2.0;

            var x = Coordinate.DistanceEstimateInMeter((float)centerY, (float)envelope.MinX, (float)centerY,
                (float)envelope.MaxX);
            var y = Coordinate.DistanceEstimateInMeter((float)envelope.MinY, (float)centerX, (float)envelope.MaxY,
                (float)centerX);
            var ratioX = width / x;
            var ratioY = height / y;
            var ratio = Math.Min(ratioX, ratioY);

            var items = new List<Item>(keys.Count);
            foreach (var item in result)
            {
                var title = item.tags.TryGetValue("name", out var name) ? name : string.Empty;

                var path = item.location is LineString lineString
                    ? lineString.ToPath(envelope, ratio, width, height)
                    : item.location is Polygon polygon
                        ? polygon.ToPath(envelope, ratio, width, height)
                        : item.location is GeometryCollection collection
                            ? collection.ToPath(envelope, ratio, width, height)
                            : string.Empty;

                items.Add(new Item
                {
                    Path = path,
                    Title = title
                });
            }

            return
                $@"<svg width=""{width}"" height=""{height}"">
{string.Join(Environment.NewLine, items.Select(i => $@"<path d=""{i.Path}"">
<title>{i.Title}</title>
</path>"))}
</svg>";
        }

        public class Item
        {
            public string Path { get; set; }
            public string Title { get; set; }
        }
    }
}