using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text;
using NetTopologySuite.Geometries;
using NetTopologySuite.Utilities;
using Coordinate = Route.LocalGeo.Coordinate;

namespace Placium.Services
{
    public static class SvgExtension
    {
        private static readonly NumberFormatInfo Nfi = new NumberFormatInfo { NumberDecimalSeparator = "." };

        public static string ToPath(this LineString lineString, double ratio, int width, int height, double centerX,
            double centerY)
        {
            var sb = new StringBuilder();

            var first = true;
            foreach (var coordinate in lineString.Coordinates)
            {
                sb.Append(first ? "M" : "L");
                var x = Math.PI * Coordinate.RadiusOfEarth * (coordinate.X - centerX) / 180d * Math.Cos(Math.PI * centerY / 180d);
                var y = Math.PI * Coordinate.RadiusOfEarth * (coordinate.Y - centerY) / 180d;
                var i = width / 2d + x * ratio;
                var j = height / 2d - y * ratio;
                sb.Append($"{i.ToString("0.00", Nfi)},{j.ToString("0.00", Nfi)}");
                first = false;
            }

            return sb.ToString();
        }

        public static string ToPath(this Point point, double ratio, int width, int height, double centerX,
            double centerY)
        {
            var latitude = point.Y;
            var longitude = point.X;
            var diameterInMeters = 10d / ratio;

            var shapeFactory = new GeometricShapeFactory
            {
                NumPoints = 16, // adjustable
                Centre = new NetTopologySuite.Geometries.Coordinate(longitude, latitude),
                // Length in meters of 1° of latitude = always 111.32 km
                Height = diameterInMeters / 111320d,
                // Length in meters of 1° of longitude = 40075 km * cos( latitude ) / 360
                Width = diameterInMeters / (40075000d * Math.Cos(Math.PI * latitude / 180d) / 360d)
            };

            var circle = shapeFactory.CreateEllipse();
            return circle.ToPath(ratio, width, height, centerX, centerY);
        }

        public static string ToPath(this Polygon polygon, double ratio, int width, int height, double centerX,
            double centerY)
        {
             var sb = new StringBuilder();

            var first = true;
            foreach (var coordinate in polygon.Coordinates)
            {
                sb.Append(first ? "M" : "L");
                var x = Math.PI * Coordinate.RadiusOfEarth * (coordinate.X - centerX) / 180d * Math.Cos(Math.PI * centerY / 180d);
                var y = Math.PI * Coordinate.RadiusOfEarth * (coordinate.Y - centerY) / 180d;
                var i = width / 2d + x * ratio;
                var j = height / 2d - y * ratio;
                sb.Append($"{i.ToString("0.00", Nfi)},{j.ToString("0.00", Nfi)}");
                first = false;
            }

            sb.Append("Z");

            return sb.ToString();
        }

        public static string ToPath(this GeometryCollection collection, double ratio, int width,
            int height, double centerX, double centerY)
        {
            var paths = new List<string>();

            if (collection.Geometries != null)
                foreach (var g in collection.Geometries)
                    switch (g)
                    {
                        case Point point:
                            paths.Add(point.ToPath(ratio, width, height, centerX, centerY));
                            break;
                        case LineString lineString:
                            paths.Add(lineString.ToPath(ratio, width, height, centerX, centerY));
                            break;
                        case Polygon polygon:
                            paths.Add(polygon.ToPath(ratio, width, height, centerX, centerY));
                            break;
                    }

            return string.Join(" ", paths);
        }
    }
}