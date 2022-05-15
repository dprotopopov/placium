using System;
using System.Collections.Generic;
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

        public async Task<string> GetMap(string name, List<string> keys, int width, int height)
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

                var g = GeometryFactory.CreateEmpty(Dimension.Surface);
                foreach (var item in result)
                {
                    var g1 = item.location;
                    item.location = g1 = g1.Buffer(0);
                    g = g.Union(g1);
                }

                var envelope = g.EnvelopeInternal;
                var centerX = (envelope.MaxX + envelope.MinX) / 2d;
                var centerY = (envelope.MaxY + envelope.MinY) / 2d;

                var x = Coordinate.DistanceEstimateInMeter((float)centerY, (float)envelope.MinX, (float)centerY,
                    (float)envelope.MaxX);
                var y = Coordinate.DistanceEstimateInMeter((float)envelope.MinY, (float)centerX, (float)envelope.MaxY,
                    (float)centerX);
                var ratioX = width / x;
                var ratioY = height / y;
                var ratio = Math.Min(ratioX, ratioY);

                var map = new Map
                {
                    Name = name,
                    Width = width,
                    Height = height
                };
                map.Paths = new List<MapItem>(keys.Count);
                foreach (var item in result)
                {
                    var envelope1 = item.location.EnvelopeInternal;

                    var minX = Coordinate.DistanceEstimateInMeter((float)centerY, (float)centerX, (float)centerY,
                        (float)envelope1.MinX);
                    var maxX = Coordinate.DistanceEstimateInMeter((float)centerY, (float)centerX, (float)centerY,
                        (float)envelope1.MaxX);
                    var minY = Coordinate.DistanceEstimateInMeter((float)centerY, (float)centerX, (float)envelope1.MinY,
                        (float)centerX);
                    var maxY = Coordinate.DistanceEstimateInMeter((float)centerY, (float)centerX, (float)envelope1.MaxY,
                        (float)centerX);

                    var left = (int)Math.Floor(width / 2d + (envelope1.MinX >= centerX ? minX : -minX) * ratio);
                    var right = (int)Math.Floor(width / 2d + (envelope1.MaxX >= centerX ? maxX : -maxX) * ratio);
                    var bottom = (int)Math.Ceiling(height / 2d - (envelope1.MinY >= centerY ? minY : -minY) * ratio);
                    var top = (int)Math.Ceiling(height / 2d - (envelope1.MaxY >= centerY ? maxY : -maxY) * ratio);
                    var rect = $"{left}, {top}, {right - left}, {bottom - top}";

                    var title = item.tags.TryGetValue("name", out var s) ? s : string.Empty;
                    var key = Regex.Replace(title, @"\W+", "", RegexOptions.CultureInvariant | RegexOptions.IgnoreCase)
                        .ToLower();

                    var data = item.location is Point point
                        ? point.ToPath(envelope, ratio, width, height)
                        : item.location is LineString lineString
                            ? lineString.ToPath(envelope, ratio, width, height)
                            : item.location is Polygon polygon
                                ? polygon.ToPath(envelope, ratio, width, height)
                                : item.location is GeometryCollection collection
                                    ? collection.ToPath(envelope, ratio, width, height)
                                    : string.Empty;

                    map.Paths.Add(new MapItem
                    {
                        Data = data,
                        EnglishName = title,
                        Key = key,
                        ISOCode = key,
                        Rect = rect,
                        RectIso = rect
                    });
                }

                return Pack(JsonConvert.SerializeObject(map));
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, ex.Message);
                throw;
            }
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
            if (string.IsNullOrWhiteSpace(str)) return null;

            return ConvertByteArrayToString(Unpack(Convert.FromBase64String(str)));
        }

        public static string Pack(string str)
        {
            if (string.IsNullOrWhiteSpace(str)) return null;
            return Convert.ToBase64String(Pack(ConvertStringToByteArray(str)));
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