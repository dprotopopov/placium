using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
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
        private static readonly GeometryFactory GeometryFactory = new GeometryFactory();
        private readonly ILogger<SvgService> _logger;

        public SvgService(IConfiguration configuration, ILogger<SvgService> logger) : base(configuration)
        {
            _logger = logger;
        }

        public async Task<string> GetSvg(List<string> osmIds, int width, int height)
        {
            await using var connection = new NpgsqlConnection(GetOsmConnectionString());
            await connection.OpenAsync();

            connection.ReloadTypes();
            connection.TypeMapper.UseNetTopologySuite();

            var types = osmIds.Select(x =>
                x.StartsWith("R", StringComparison.InvariantCultureIgnoreCase) ? "relation" :
                x.StartsWith("W", StringComparison.InvariantCultureIgnoreCase) ? "way" :
                x.StartsWith("N", StringComparison.InvariantCultureIgnoreCase) ? "node" :
                throw new ArgumentException()).ToArray();
            var ids = osmIds.Select(x => long.Parse(x[1..])).ToArray();

            var result = new List<Placex>(osmIds.Count);

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

            var g = GeometryFactory.CreateEmpty(Dimension.Surface);
            foreach (var item in result)
            {
                var title = item.tags.TryGetValue("name:ru", out var name) || item.tags.TryGetValue("name", out name) ? name : string.Empty;

                _logger.LogDebug($"Union begin {title} {item.location.GetType().Name}");

                var g1 = item.location;
                g = g.Union(g1.Buffer(0));

                _logger.LogDebug($"Union complete {title} {item.location.GetType().Name}");
            }

            var maxX = g.Coordinates.Max(p => p.X);
            var minX = g.Coordinates.Min(p => p.X);
            var maxY = g.Coordinates.Max(p => p.Y);
            var minY = g.Coordinates.Min(p => p.Y);
            var centerX = (maxX + minX) / 2d;
            var centerY = (maxY + minY) / 2d;

            var x = Math.PI * Coordinate.RadiusOfEarth * (maxX - minX) / 180d * Math.Cos(Math.PI * centerY / 180d);
            var y = Math.PI * Coordinate.RadiusOfEarth * (maxY - minY) / 180d;
            var ratioX = width / x;
            var ratioY = height / y;
            var ratio = Math.Min(ratioX, ratioY);

            var items = new List<Item>(osmIds.Count);
            foreach (var item in result)
            {
                var title = item.tags.TryGetValue("name:ru", out var name) || item.tags.TryGetValue("name", out name) ? name : string.Empty;
                var key = Regex.Replace(title, @"\W+", "", RegexOptions.CultureInvariant | RegexOptions.IgnoreCase)
                    .ToLower();

                _logger.LogDebug($"Generate path begin {title} {item.location.GetType().Name}");

                var path = item.location switch
                {
                    Point point => point.ToPath(ratio, width, height, centerX, centerY),
                    LineString lineString => lineString.ToPath(ratio, width, height, centerX, centerY),
                    Polygon polygon => polygon.ToPath(ratio, width, height, centerX, centerY),
                    GeometryCollection collection => collection.ToPath(ratio, width, height, centerX, centerY),
                    _ => string.Empty
                };

                _logger.LogDebug($"Generate path complete {title} {item.location.GetType().Name}");

                items.Add(new Item
                {
                    Path = path,
                    Title = title,
                    Key = key
                });
            }

            var merged = items.GroupBy(item => item.Key).Select(grouping => new Item
            {
                Key = grouping.Key,
                Title = grouping.First().Title,
                Path = string.Join(" ", grouping.Select(item => item.Path))
            }).ToList();

            return
                $@"<svg width=""{width}"" height=""{height}"">
{string.Join(Environment.NewLine, merged.Select(i => $@"<path d=""{i.Path}"">
<title>{i.Title}</title>
</path>"))}
</svg>";
        }

        public class Item
        {
            public string Path { get; set; }
            public string Title { get; set; }
            public string Key { get; set; }
        }
    }
}