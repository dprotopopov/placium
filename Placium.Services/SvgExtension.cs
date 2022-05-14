using System.Collections.Generic;
using System.Globalization;
using System.Text;
using NetTopologySuite.Geometries;
using Coordinate = Route.LocalGeo.Coordinate;

namespace Placium.Services
{
    public static class SvgExtension
    {
        private static readonly NumberFormatInfo Nfi = new NumberFormatInfo { NumberDecimalSeparator = "." };

        public static string ToPath(this LineString lineString, Envelope envelope, double ratio, int width, int height)
        {
            var centerX = (envelope.MaxX + envelope.MinX) / 2.0;
            var centerY = (envelope.MaxY + envelope.MinY) / 2.0;
            var sb = new StringBuilder();

            var first = true;
            foreach (var coordinate in lineString.Coordinates)
            {
                sb.Append(first ? "M" : "L");
                var x = Coordinate.DistanceEstimateInMeter((float)centerY, (float)centerX, (float)centerY,
                    (float)coordinate.X);
                var y = Coordinate.DistanceEstimateInMeter((float)centerY, (float)centerX, (float)coordinate.Y,
                    (float)centerX);
                var i = width / 2.0 + (coordinate.X >= centerX ? x : -x) * ratio;
                var j = height / 2.0 - (coordinate.Y >= centerY ? y : -y) * ratio;
                sb.Append($"{i.ToString("0.00", Nfi)},{j.ToString("0.00", Nfi)}");
                first = false;
            }

            return sb.ToString();
        }

        public static string ToPath(this Polygon polygon, Envelope envelope, double ratio, int width, int height)
        {
            var centerX = (envelope.MaxX + envelope.MinX) / 2.0;
            var centerY = (envelope.MaxY + envelope.MinY) / 2.0;
            var sb = new StringBuilder();

            var first = true;
            foreach (var coordinate in polygon.Coordinates)
            {
                sb.Append(first ? "M" : "L");
                var x = Coordinate.DistanceEstimateInMeter((float)centerY, (float)centerX, (float)centerY,
                    (float)coordinate.X);
                var y = Coordinate.DistanceEstimateInMeter((float)centerY, (float)centerX, (float)coordinate.Y,
                    (float)centerX);
                var i = width / 2.0 + (coordinate.X >= centerX ? x : -x) * ratio;
                var j = height / 2.0 - (coordinate.Y >= centerY ? y : -y) * ratio;
                sb.Append($"{i.ToString("0.00", Nfi)},{j.ToString("0.00", Nfi)}");
                first = false;
            }

            sb.Append("Z");

            return sb.ToString();
        }

        public static string ToPath(this GeometryCollection collection, Envelope envelope, double ratio, int width,
            int height)
        {
            var paths = new List<string>();

            foreach (var g in collection.Geometries)
                switch (g)
                {
                    case LineString lineString:
                        paths.Add(lineString.ToPath(envelope, ratio, width, height));
                        break;
                    case Polygon polygon:
                        paths.Add(polygon.ToPath(envelope, ratio, width, height));
                        break;
                }

            return string.Join(" ", paths);
        }
    }
}