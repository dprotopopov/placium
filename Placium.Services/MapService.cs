using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using NetTopologySuite.Geometries;
using Newtonsoft.Json;
using Npgsql;
using Placium.Common;
using Placium.Models;
using Coordinate = Route.LocalGeo.Coordinate;

namespace Placium.Services
{
    public class MapService : BaseApiService
    {
        private static readonly GeometryFactory GeometryFactory = new GeometryFactory();
        private readonly ILogger<MapService> _logger;

        public MapService(IConfiguration configuration, ILogger<MapService> logger) : base(configuration)
        {
            _logger = logger;
        }

        public async Task<string> GetMap(string name, List<string> osmIds, int width, int height, bool json = false)
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

                var g = GeometryFactory.CreateEmpty(Dimension.Surface);
                foreach (var item in result)
                {
                    var g1 = item.location;
                    g = g.Union(g1.Buffer(0));
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

                var map = new Map
                {
                    Name = name,
                    Width = width,
                    Height = height
                };
                var items = new List<MapItem>(osmIds.Count);
                foreach (var item in result)
                {
                    var envelope1 = item.location.EnvelopeInternal;
                    var maxX1 = item.location.Coordinates.Max(p => p.X);
                    var minX1 = item.location.Coordinates.Min(p => p.X);
                    var maxY1 = item.location.Coordinates.Max(p => p.Y);
                    var minY1 = item.location.Coordinates.Min(p => p.Y);
                    var centerX1 = (maxX1 + minX1) / 2d;
                    var centerY1 = (maxY1 + minY1) / 2d;

                    var x1 = Math.PI * Coordinate.RadiusOfEarth * (minX1 - centerX) / 180d * Math.Cos(Math.PI * centerY / 180d);
                    var x2 = Math.PI * Coordinate.RadiusOfEarth * (maxX1 - centerX) / 180d * Math.Cos(Math.PI * centerY / 180d);
                    var y1 = Math.PI * Coordinate.RadiusOfEarth * (minY1 - centerY) / 180d;
                    var y2 = Math.PI * Coordinate.RadiusOfEarth * (maxY1 - centerY) / 180d;


                    var left = (int)Math.Floor(width / 2d + x1 * ratio);
                    var right = (int)Math.Floor(width / 2d + x2 * ratio);
                    var bottom = (int)Math.Ceiling(height / 2d - y1 * ratio);
                    var top = (int)Math.Ceiling(height / 2d - y2 * ratio);
                    var rect = $"{left}, {top}, {right - left}, {bottom - top}";

                    var title = item.tags.TryGetValue("name", out var s) ? s : string.Empty;
                    var key = Regex.Replace(title, @"\W+", "", RegexOptions.CultureInvariant | RegexOptions.IgnoreCase)
                        .ToLower();

                    var data = item.location switch
                    {
                        Point point => point.ToPath(ratio, width, height, centerX, centerY),
                        LineString lineString => lineString.ToPath(ratio, width, height, centerX, centerY),
                        Polygon polygon => polygon.ToPath(ratio, width, height, centerX, centerY),
                        GeometryCollection collection => collection.ToPath(ratio, width, height, centerX, centerY),
                        _ => string.Empty
                    };

                    items.Add(new MapItem
                    {
                        Data = data,
                        EnglishName = title,
                        Key = key,
                        ISOCode = key,
                        Rect = rect,
                        RectIso = rect
                    });
                }

                var merged = items.GroupBy(item => item.Key).Select(grouping => new MapItem
                {
                    Key = grouping.Key,
                    EnglishName = grouping.First().EnglishName,
                    ISOCode = grouping.First().ISOCode,
                    Rect = UnionRect(grouping.Select(x => x.Rect).ToList()),
                    RectIso = UnionRect(grouping.Select(x => x.RectIso).ToList()),
                    Data = string.Join(" ", grouping.Select(item => item.Data))
                }).ToList();

                map.Paths = merged;

                var str = JsonConvert.SerializeObject(map, Formatting.Indented);
                if(!json) str = Pack(str);
                return str;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, ex.FullMessage());
                throw;
            }
        }

        public string UnionRect(IEnumerable<string> rects)
        {
            var first = true;
            int[] united = null;
            foreach (var rect in rects)
            {
                var arr = JsonConvert.DeserializeObject<int[]>($"[{rect}]");

                Debug.Assert(arr is { Length: 4 });

                if (first)
                {
                    united = new[] { arr[0], arr[1], arr[0] + arr[2], arr[1] + arr[3] };
                }
                else
                {
                    united[0] = Math.Min(united[0], arr[0]);
                    united[1] = Math.Min(united[1], arr[1]);
                    united[2] = Math.Max(united[2], arr[0] + arr[2]);
                    united[3] = Math.Max(united[3], arr[1] + arr[3]);
                }

                first = false;
            }

            Debug.Assert(united is { Length: 4 });

            return $"{united[0]}, {united[1]}, {united[2] - united[0]}, {united[3] - united[1]}";
        }

        public static byte[] ConvertStringToByteArray(string str)
        {
            if (str == null) return null;

            return Encoding.UTF8.GetBytes(str);
        }

        public static string ConvertByteArrayToString(byte[] bytes)
        {
            if (bytes == null) return null;

            using var memoryStream = new MemoryStream(bytes);
            using var reader = new StreamReader(memoryStream);
            var str = reader.ReadToEnd();
            reader.Close();
            memoryStream.Flush();
            memoryStream.Close();
            return str;
        }

        public static string Unpack(string str)
        {
            return string.IsNullOrWhiteSpace(str)
                ? null
                : ConvertByteArrayToString(Unpack(Convert.FromBase64String(str)));
        }

        public static string Pack(string str)
        {
            return string.IsNullOrWhiteSpace(str)
                ? null
                : Convert.ToBase64String(Pack(ConvertStringToByteArray(str)));
        }

        public static byte[] Unpack(byte[] bytes)
        {
            using var memoryStream = new MemoryStream(bytes);
            using var zipStream = new GZipStream(memoryStream, CompressionMode.Decompress);
            using var resultMemoryStream = new MemoryStream();
            zipStream.CopyTo(resultMemoryStream);
            return resultMemoryStream.ToArray();
        }

        public static byte[] Pack(byte[] bytes)
        {
            using var resultMemoryStream = new MemoryStream();
            using var zipStream = new GZipStream(resultMemoryStream, CompressionMode.Compress);
            using var memoryStream = new MemoryStream(bytes);
            memoryStream.CopyTo(zipStream);
            zipStream.Flush();
            return resultMemoryStream.ToArray();
        }

        public class Item
        {
            public string Path { get; set; }
            public string Title { get; set; }
        }

        public class Map
        {
            public string Name { get; set; }
            public int Width { get; set; }
            public int Height { get; set; }
            public List<MapItem> Paths { get; set; }
        }

        public class MapItem
        {
            public string Key { get; set; }
            public string EnglishName { get; set; }
            public string Data { get; set; }
            public string ISOCode { get; set; }
            public string Rect { get; set; }
            public bool SetMaxWidth { get; set; } = false;
            public bool SkipText { get; set; } = false;
            public string HorAlignment { get; set; } = "Center";
            public string VertAlignment { get; set; } = "Center";
            public string RectIso { get; set; }
            public bool SkipTextIso { get; set; } = false;
            public string HorAlignmentIso { get; set; } = "Center";
            public string VertAlignmentIso { get; set; } = "Center";
            public decimal ShowHiddenTextIfZoom { get; set; } = 0.2m;
        }
    }
}