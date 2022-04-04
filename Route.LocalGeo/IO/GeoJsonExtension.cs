﻿using System;
using System.Collections.Generic;
using System.IO;
using Route.IO.Json;

namespace Route.LocalGeo.IO
{
    /// <summary>
    ///     Contains extension methods for the local geo objects related to the GeoJSON format.
    /// </summary>
    public static class GeoJsonExtension
    {
        /// <summary>
        ///     Serializes the polygon to GeoJSON.
        /// </summary>
        public static string ToGeoJson(this Polygon polygon)
        {
            var stringWriter = new StringWriter();
            polygon.WriteGeoJson(stringWriter);
            return stringWriter.ToInvariantString();
        }

        /// <summary>
        ///     Serializes the polygons to GeoJSON.
        /// </summary>
        public static string ToGeoJson(this IEnumerable<Polygon> polygons)
        {
            var stringWriter = new StringWriter();
            polygons.WriteGeoJson(stringWriter);
            return stringWriter.ToInvariantString();
        }

        /// <summary>
        ///     Writes the polygon as geojson.
        /// </summary>
        public static void WriteGeoJson(this IEnumerable<Polygon> polygons, TextWriter writer)
        {
            if (polygons == null) throw new ArgumentNullException("polygons");
            if (writer == null) throw new ArgumentNullException("writer");

            var jsonWriter = new JsonWriter(writer);
            jsonWriter.WriteOpen();
            jsonWriter.WriteProperty("type", "FeatureCollection", true);
            jsonWriter.WritePropertyName("features");
            jsonWriter.WriteArrayOpen();

            foreach (var polygon in polygons)
            {
                jsonWriter.WriteOpen();
                jsonWriter.WriteProperty("type", "Feature", true);
                jsonWriter.WriteProperty("name", "Shape", true);
                jsonWriter.WritePropertyName("properties");
                jsonWriter.WriteOpen();
                jsonWriter.WriteClose();
                jsonWriter.WritePropertyName("geometry");

                polygon.WriteGeoJson(jsonWriter);

                jsonWriter.WriteClose(); // closes the feature.
            }

            jsonWriter.WriteArrayClose(); // closes the feature array.
            jsonWriter.WriteClose(); // closes the feature collection.
        }

        /// <summary>
        ///     Writes the polygon as geojson.
        /// </summary>
        public static void WriteGeoJson(this Polygon polygon, TextWriter writer)
        {
            if (polygon == null) throw new ArgumentNullException("polygon");
            if (writer == null) throw new ArgumentNullException("writer");
            if (polygon.ExteriorRing == null) throw new ArgumentNullException("polygon.ExteriorRing");

            var jsonWriter = new JsonWriter(writer);
            jsonWriter.WriteOpen();
            jsonWriter.WriteProperty("type", "FeatureCollection", true);
            jsonWriter.WritePropertyName("features");
            jsonWriter.WriteArrayOpen();

            jsonWriter.WriteOpen();
            jsonWriter.WriteProperty("type", "Feature", true);
            jsonWriter.WriteProperty("name", "Shape", true);
            jsonWriter.WritePropertyName("properties");
            jsonWriter.WriteOpen();
            jsonWriter.WriteClose();
            jsonWriter.WritePropertyName("geometry");

            polygon.WriteGeoJson(jsonWriter);

            jsonWriter.WriteClose(); // closes the feature.
            jsonWriter.WriteArrayClose(); // closes the feature array.
            jsonWriter.WriteClose(); // closes the feature collection.
        }

        /// <summary>
        ///     Writes the polygon as geojson.
        /// </summary>
        private static void WriteGeoJson(this Polygon polygon, JsonWriter jsonWriter)
        {
            if (polygon == null) throw new ArgumentNullException("polygon");
            if (jsonWriter == null) throw new ArgumentNullException("jsonWriter");
            if (polygon.ExteriorRing == null) throw new ArgumentNullException("polygon.ExteriorRing");

            jsonWriter.WriteOpen();
            jsonWriter.WriteProperty("type", "Polygon", true);
            jsonWriter.WritePropertyName("coordinates");
            jsonWriter.WriteArrayOpen();

            jsonWriter.WriteArrayOpen();
            for (var i = 0; i < polygon.ExteriorRing.Count; i++)
            {
                jsonWriter.WriteArrayOpen();
                jsonWriter.WriteArrayValue(polygon.ExteriorRing[i].Longitude.ToInvariantString());
                jsonWriter.WriteArrayValue(polygon.ExteriorRing[i].Latitude.ToInvariantString());
                jsonWriter.WriteArrayClose();
            }

            jsonWriter.WriteArrayClose();

            if (polygon.InteriorRings != null)
                foreach (var interior in polygon.InteriorRings)
                {
                    jsonWriter.WriteArrayOpen();
                    for (var i = 0; i < interior.Count; i++)
                    {
                        jsonWriter.WriteArrayOpen();
                        jsonWriter.WriteArrayValue(interior[i].Longitude.ToInvariantString());
                        jsonWriter.WriteArrayValue(interior[i].Latitude.ToInvariantString());
                        jsonWriter.WriteArrayClose();
                    }

                    jsonWriter.WriteArrayClose();
                }

            jsonWriter.WriteArrayClose(); // closes the coordinates top level.
            jsonWriter.WriteClose(); // closes the geometry.
        }

        /// <summary>
        ///     Converts all the lines to geojson.
        /// </summary>
        public static string ToGeoJson(this IEnumerable<Tuple<float, float, List<Coordinate>>> lines)
        {
            var stringWriter = new StringWriter();
            lines.WriteGeoJson(stringWriter);
            return stringWriter.ToInvariantString();
        }

        /// <summary>
        ///     Writes the lines as geojson.
        /// </summary>
        private static void WriteGeoJson(this IEnumerable<Tuple<float, float, List<Coordinate>>> lines,
            TextWriter writer)
        {
            if (lines == null) throw new ArgumentNullException("lines");
            if (writer == null) throw new ArgumentNullException("writer");

            var jsonWriter = new JsonWriter(writer);
            jsonWriter.WriteOpen();
            jsonWriter.WriteProperty("type", "FeatureCollection", true);
            jsonWriter.WritePropertyName("features");
            jsonWriter.WriteArrayOpen();

            foreach (var line in lines)
            {
                jsonWriter.WriteOpen();
                jsonWriter.WriteProperty("type", "Feature", true);
                jsonWriter.WriteProperty("name", "Shape", true);
                jsonWriter.WritePropertyName("properties");
                jsonWriter.WriteOpen();

                jsonWriter.WritePropertyName("start_weight");
                jsonWriter.WritePropertyValue(line.Item1.ToInvariantString());
                jsonWriter.WritePropertyName("end_weight");
                jsonWriter.WritePropertyValue(line.Item2.ToInvariantString());

                jsonWriter.WriteClose();
                jsonWriter.WritePropertyName("geometry");

                line.Item3.WriteGeoJson(jsonWriter);

                jsonWriter.WriteClose(); // closes the feature.
            }

            jsonWriter.WriteArrayClose(); // closes the feature array.
            jsonWriter.WriteClose(); // closes the feature collection.
        }

        /// <summary>
        ///     Writes the line as geojson.
        /// </summary>
        private static void WriteGeoJson(this List<Coordinate> line, JsonWriter jsonWriter)
        {
            if (line == null) throw new ArgumentNullException("line");
            if (jsonWriter == null) throw new ArgumentNullException("jsonWriter");

            jsonWriter.WriteOpen();
            jsonWriter.WriteProperty("type", "LineString", true);
            jsonWriter.WritePropertyName("coordinates");
            jsonWriter.WriteArrayOpen();

            for (var i = 0; i < line.Count; i++)
            {
                jsonWriter.WriteArrayOpen();
                jsonWriter.WriteArrayValue(line[i].Longitude.ToInvariantString());
                jsonWriter.WriteArrayValue(line[i].Latitude.ToInvariantString());
                jsonWriter.WriteArrayClose();
            }

            jsonWriter.WriteArrayClose(); // closes the coordinates top level.
            jsonWriter.WriteClose(); // closes the geometry.
        }

        /// <summary>
        ///     Serializes the line to GeoJSON.
        /// </summary>
        public static string ToGeoJson(this Line line)
        {
            var stringWriter = new StringWriter();
            var jsonWriter = new JsonWriter(stringWriter);
            new List<Coordinate> { line.Coordinate1, line.Coordinate2 }.WriteGeoJson(jsonWriter);
            return stringWriter.ToInvariantString();
        }

        /// <summary>
        ///     Serializes the coordinate to a point in GeoJSON.
        /// </summary>
        public static string ToGeoJson(this Coordinate coordinate)
        {
            var stringWriter = new StringWriter();
            var jsonWriter = new JsonWriter(stringWriter);
            coordinate.WriteGeoJson(jsonWriter);
            return stringWriter.ToInvariantString();
        }

        private static void WriteGeoJson(this Coordinate point, JsonWriter jsonWriter)
        {
            if (jsonWriter == null) throw new ArgumentNullException("jsonWriter");

            jsonWriter.WriteOpen();
            jsonWriter.WriteProperty("type", "Point", true);
            jsonWriter.WritePropertyName("coordinates");
            jsonWriter.WriteArrayOpen();
            jsonWriter.WriteArrayValue(point.Longitude.ToInvariantString());
            jsonWriter.WriteArrayValue(point.Latitude.ToInvariantString());
            jsonWriter.WriteArrayClose();
            jsonWriter.WriteClose(); // closes the geometry.
        }
    }
}