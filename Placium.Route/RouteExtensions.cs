using System;
using System.Collections.Generic;
using System.IO;
using Route.Attributes;
using Route.IO.Json;
using Route.LocalGeo;

namespace Placium.Route
{
    public static class RouteExtensions
    {
        /// <summary>
        ///     Returns this route as json.
        /// </summary>
        public static string ToJson(this Route route)
        {
            var stringWriter = new StringWriter();
            route.WriteJson(stringWriter);
            return Extensions.ToInvariantString(stringWriter);
        }

        /// <summary>
        ///     Writes the route as json.
        /// </summary>
        public static void WriteJson(this Route route, Stream stream)
        {
            route.WriteJson(new StreamWriter(stream));
        }

        /// <summary>
        ///     Writes the route as json.
        /// </summary>
        public static void WriteJson(this Route route, TextWriter writer)
        {
            if (route == null) throw new ArgumentNullException(nameof(route));
            if (writer == null) throw new ArgumentNullException(nameof(writer));

            var jsonWriter = new JsonWriter(writer);
            jsonWriter.WriteOpen();
            if (route.Attributes != null)
            {
                jsonWriter.WritePropertyName("Attributes");
                jsonWriter.WriteOpen();
                foreach (var attribute in route.Attributes)
                    jsonWriter.WriteProperty(attribute.Key, attribute.Value, true, true);

                jsonWriter.WriteClose();
            }

            if (route.Shape != null)
            {
                jsonWriter.WritePropertyName("Shape");
                jsonWriter.WriteArrayOpen();
                for (var i = 0; i < route.Shape.Length; i++)
                {
                    jsonWriter.WriteArrayOpen();
                    jsonWriter.WriteArrayValue(Extensions.ToInvariantString(route.Shape[i].Longitude));
                    jsonWriter.WriteArrayValue(Extensions.ToInvariantString(route.Shape[i].Latitude));
                    jsonWriter.WriteArrayClose();
                }

                jsonWriter.WriteArrayClose();
            }

            if (route.ShapeMeta != null)
            {
                jsonWriter.WritePropertyName("ShapeMeta");
                jsonWriter.WriteArrayOpen();
                for (var i = 0; i < route.ShapeMeta.Length; i++)
                {
                    var meta = route.ShapeMeta[i];

                    jsonWriter.WriteOpen();
                    jsonWriter.WritePropertyName("Shape");
                    jsonWriter.WritePropertyValue(Extensions.ToInvariantString(meta.Shape));

                    if (meta.Attributes != null)
                    {
                        jsonWriter.WritePropertyName("Attributes");
                        jsonWriter.WriteOpen();
                        foreach (var attribute in meta.Attributes)
                            jsonWriter.WriteProperty(attribute.Key, attribute.Value, true, true);
                        jsonWriter.WriteClose();
                    }

                    jsonWriter.WriteClose();
                }

                jsonWriter.WriteArrayClose();
            }

            if (route.Stops != null)
            {
                jsonWriter.WritePropertyName("Stops");
                jsonWriter.WriteArrayOpen();
                for (var i = 0; i < route.Stops.Length; i++)
                {
                    var stop = route.Stops[i];

                    jsonWriter.WriteOpen();
                    jsonWriter.WritePropertyName("Shape");
                    jsonWriter.WritePropertyValue(Extensions.ToInvariantString(stop.Shape));
                    jsonWriter.WritePropertyName("Coordinates");
                    jsonWriter.WriteArrayOpen();
                    jsonWriter.WriteArrayValue(Extensions.ToInvariantString(route.Stops[i].Coordinate.Longitude));
                    jsonWriter.WriteArrayValue(Extensions.ToInvariantString(route.Stops[i].Coordinate.Latitude));
                    jsonWriter.WriteArrayClose();

                    if (stop.Attributes != null)
                    {
                        jsonWriter.WritePropertyName("Attributes");
                        jsonWriter.WriteOpen();
                        foreach (var attribute in stop.Attributes)
                            jsonWriter.WriteProperty(attribute.Key, attribute.Value, true, true);
                        jsonWriter.WriteClose();
                    }

                    jsonWriter.WriteClose();
                }

                jsonWriter.WriteArrayClose();
            }

            if (route.Branches != null)
            {
                jsonWriter.WritePropertyName("Branches");
                jsonWriter.WriteArrayOpen();
                for (var i = 0; i < route.Branches.Length; i++)
                {
                    var stop = route.Branches[i];

                    jsonWriter.WriteOpen();
                    jsonWriter.WritePropertyName("Shape");
                    jsonWriter.WritePropertyValue(Extensions.ToInvariantString(stop.Shape));
                    jsonWriter.WritePropertyName("Coordinates");
                    jsonWriter.WriteArrayOpen();
                    jsonWriter.WriteArrayValue(Extensions.ToInvariantString(route.Branches[i].Coordinate.Longitude));
                    jsonWriter.WriteArrayValue(Extensions.ToInvariantString(route.Branches[i].Coordinate.Latitude));
                    jsonWriter.WriteArrayClose();

                    if (stop.Attributes != null)
                    {
                        jsonWriter.WritePropertyName("Attributes");
                        jsonWriter.WriteOpen();
                        foreach (var attribute in stop.Attributes)
                            jsonWriter.WriteProperty(attribute.Key, attribute.Value, true, true);
                        jsonWriter.WriteClose();
                    }

                    jsonWriter.WriteClose();
                }

                jsonWriter.WriteArrayClose();
            }

            jsonWriter.WriteClose();
        }

        /// <summary>
        ///     Writes the route as geojson.
        /// </summary>
        public static void WriteGeoJson(this Route route, TextWriter writer, bool includeShapeMeta = true,
            bool includeStops = true, bool groupByShapeMeta = true,
            Action<IAttributeCollection> attributesCallback = null, Func<string, string, bool> isRaw = null)
        {
            if (route == null) throw new ArgumentNullException("route");
            if (writer == null) throw new ArgumentNullException("writer");

            var jsonWriter = new JsonWriter(writer);
            jsonWriter.WriteOpen();
            jsonWriter.WriteProperty("type", "FeatureCollection", true);
            jsonWriter.WritePropertyName("features");
            jsonWriter.WriteArrayOpen();

            route.WriteGeoJsonFeatures(jsonWriter, includeShapeMeta, includeStops, groupByShapeMeta, attributesCallback,
                isRaw);

            jsonWriter.WriteArrayClose();
            jsonWriter.WriteClose();
        }

        /// <summary>
        ///     Returns this route as geojson.
        /// </summary>
        public static string ToGeoJson(this Route route, bool includeShapeMeta = true, bool includeStops = true,
            bool groupByShapeMeta = true,
            Action<IAttributeCollection> attributesCallback = null, Func<string, string, bool> isRaw = null)
        {
            var stringWriter = new StringWriter();
            route.WriteGeoJson(stringWriter, includeShapeMeta, includeStops, groupByShapeMeta, attributesCallback,
                isRaw);
            return Extensions.ToInvariantString(stringWriter);
        }

        /// <summary>
        ///     Writes the route as geojson.
        /// </summary>
        public static void WriteGeoJsonFeatures(this Route route, JsonWriter jsonWriter, bool includeShapeMeta = true,
            bool includeStops = true, bool groupByShapeMeta = true,
            Action<IAttributeCollection> attributesCallback = null, Func<string, string, bool> isRaw = null)
        {
            if (route == null) throw new ArgumentNullException("route");
            if (jsonWriter == null) throw new ArgumentNullException("jsonWriter");

            if (groupByShapeMeta)
            {
                // group by shape meta.
                if (route.Shape != null && route.ShapeMeta != null)
                    for (var i = 0; i < route.ShapeMeta.Length; i++)
                    {
                        var shapeMeta = route.ShapeMeta[i];
                        var lowerShape = -1;
                        if (i > 0) lowerShape = route.ShapeMeta[i - 1].Shape;
                        var higherShape = route.ShapeMeta[i].Shape;
                        if (lowerShape >= higherShape)
                            throw new Exception(string.Format("Invalid route: {0}", route.ToJson()));

                        var coordinates = new List<Coordinate>();
                        for (var shape = lowerShape; shape <= higherShape; shape++)
                            if (shape >= 0 && shape < route.Shape.Length)
                                coordinates.Add(route.Shape[shape]);

                        if (coordinates.Count >= 2)
                        {
                            jsonWriter.WriteOpen();
                            jsonWriter.WriteProperty("type", "Feature", true);
                            jsonWriter.WriteProperty("name", "ShapeMeta", true);
                            jsonWriter.WritePropertyName("geometry");

                            jsonWriter.WriteOpen();
                            jsonWriter.WriteProperty("type", "LineString", true);
                            jsonWriter.WritePropertyName("coordinates");
                            jsonWriter.WriteArrayOpen();

                            for (var shape = 0; shape < coordinates.Count; shape++)
                            {
                                jsonWriter.WriteArrayOpen();
                                jsonWriter.WriteArrayValue(Extensions.ToInvariantString(coordinates[shape].Longitude));
                                jsonWriter.WriteArrayValue(Extensions.ToInvariantString(coordinates[shape].Latitude));
                                if (coordinates[shape].Elevation.HasValue)
                                    jsonWriter.WriteArrayValue(Extensions.ToInvariantString(coordinates[shape].Elevation.Value));
                                jsonWriter.WriteArrayClose();
                            }

                            jsonWriter.WriteArrayClose();
                            jsonWriter.WriteClose();

                            jsonWriter.WritePropertyName("properties");
                            jsonWriter.WriteOpen();
                            if (shapeMeta.Attributes != null)
                            {
                                var attributes = shapeMeta.Attributes;
                                if (attributesCallback != null)
                                {
                                    attributes = new AttributeCollection(attributes);
                                    attributesCallback(attributes);
                                }

                                foreach (var attribute in attributes)
                                {
                                    var raw = isRaw != null &&
                                              isRaw(attribute.Key, attribute.Value);
                                    jsonWriter.WriteProperty(attribute.Key, attribute.Value, !raw, !raw);
                                }
                            }

                            jsonWriter.WriteClose();

                            jsonWriter.WriteClose();
                        }
                    }

                if (route.Stops != null &&
                    includeStops)
                    for (var i = 0; i < route.Stops.Length; i++)
                    {
                        var stop = route.Stops[i];

                        jsonWriter.WriteOpen();
                        jsonWriter.WriteProperty("type", "Feature", true);
                        jsonWriter.WriteProperty("name", "Stop", true);
                        jsonWriter.WriteProperty("Shape", Extensions.ToInvariantString(stop.Shape), true);
                        jsonWriter.WritePropertyName("geometry");

                        jsonWriter.WriteOpen();
                        jsonWriter.WriteProperty("type", "Point", true);
                        jsonWriter.WritePropertyName("coordinates");
                        jsonWriter.WriteArrayOpen();
                        jsonWriter.WriteArrayValue(Extensions.ToInvariantString(stop.Coordinate.Longitude));
                        jsonWriter.WriteArrayValue(Extensions.ToInvariantString(stop.Coordinate.Latitude));
                        if (stop.Coordinate.Elevation.HasValue)
                            jsonWriter.WriteArrayValue(Extensions.ToInvariantString(stop.Coordinate.Elevation.Value));
                        jsonWriter.WriteArrayClose();
                        jsonWriter.WriteClose();

                        jsonWriter.WritePropertyName("properties");
                        jsonWriter.WriteOpen();
                        if (stop.Attributes != null)
                        {
                            var attributes = stop.Attributes;
                            if (attributesCallback != null)
                            {
                                attributes = new AttributeCollection(attributes);
                                attributesCallback(attributes);
                            }

                            foreach (var attribute in attributes)
                            {
                                var raw = isRaw != null &&
                                          isRaw(attribute.Key, attribute.Value);
                                jsonWriter.WriteProperty(attribute.Key, attribute.Value, !raw, !raw);
                            }
                        }

                        jsonWriter.WriteClose();

                        jsonWriter.WriteClose();
                    }
            }
            else
            {
                // include shape meta as points if requested.
                if (route.Shape != null)
                {
                    jsonWriter.WriteOpen();
                    jsonWriter.WriteProperty("type", "Feature", true);
                    jsonWriter.WriteProperty("name", "Shape", true);
                    jsonWriter.WritePropertyName("properties");
                    jsonWriter.WriteOpen();
                    jsonWriter.WriteClose();
                    jsonWriter.WritePropertyName("geometry");

                    jsonWriter.WriteOpen();
                    jsonWriter.WriteProperty("type", "LineString", true);
                    jsonWriter.WritePropertyName("coordinates");
                    jsonWriter.WriteArrayOpen();
                    for (var i = 0; i < route.Shape.Length; i++)
                    {
                        jsonWriter.WriteArrayOpen();
                        jsonWriter.WriteArrayValue(Extensions.ToInvariantString(route.Shape[i].Longitude));
                        jsonWriter.WriteArrayValue(Extensions.ToInvariantString(route.Shape[i].Latitude));
                        if (route.Shape[i].Elevation.HasValue)
                            jsonWriter.WriteArrayValue(Extensions.ToInvariantString(route.Shape[i].Elevation.Value));
                        jsonWriter.WriteArrayClose();
                    }

                    jsonWriter.WriteArrayClose();
                    jsonWriter.WriteClose();

                    if (attributesCallback != null)
                    {
                        jsonWriter.WritePropertyName("properties");
                        jsonWriter.WriteOpen();
                        var attributes = new AttributeCollection();
                        attributesCallback(attributes);
                        foreach (var attribute in attributes)
                        {
                            var raw = isRaw != null &&
                                      isRaw(attribute.Key, attribute.Value);
                            jsonWriter.WriteProperty(attribute.Key, attribute.Value, !raw, !raw);
                        }

                        jsonWriter.WriteClose();
                    }

                    jsonWriter.WriteClose();
                }

                if (route.ShapeMeta != null &&
                    includeShapeMeta)
                    for (var i = 0; i < route.ShapeMeta.Length; i++)
                    {
                        var meta = route.ShapeMeta[i];

                        jsonWriter.WriteOpen();
                        jsonWriter.WriteProperty("type", "Feature", true);
                        jsonWriter.WriteProperty("name", "ShapeMeta", true);
                        jsonWriter.WriteProperty("Shape", Extensions.ToInvariantString(meta.Shape), true);
                        jsonWriter.WritePropertyName("geometry");

                        var coordinate = route.Shape[meta.Shape];

                        jsonWriter.WriteOpen();
                        jsonWriter.WriteProperty("type", "Point", true);
                        jsonWriter.WritePropertyName("coordinates");
                        jsonWriter.WriteArrayOpen();
                        jsonWriter.WriteArrayValue(Extensions.ToInvariantString(coordinate.Longitude));
                        jsonWriter.WriteArrayValue(Extensions.ToInvariantString(coordinate.Latitude));
                        if (coordinate.Elevation.HasValue)
                            jsonWriter.WriteArrayValue(Extensions.ToInvariantString(coordinate.Elevation.Value));
                        jsonWriter.WriteArrayClose();
                        jsonWriter.WriteClose();

                        jsonWriter.WritePropertyName("properties");
                        jsonWriter.WriteOpen();

                        if (meta.Attributes != null)
                        {
                            var attributes = meta.Attributes;
                            if (attributesCallback != null)
                            {
                                attributes = new AttributeCollection(attributes);
                                attributesCallback(attributes);
                            }

                            foreach (var attribute in attributes)
                            {
                                var raw = isRaw != null &&
                                          isRaw(attribute.Key, attribute.Value);
                                jsonWriter.WriteProperty(attribute.Key, attribute.Value, !raw, !raw);
                            }
                        }

                        jsonWriter.WriteClose();

                        jsonWriter.WriteClose();
                    }

                if (route.Stops != null &&
                    includeStops)
                    for (var i = 0; i < route.Stops.Length; i++)
                    {
                        var stop = route.Stops[i];

                        jsonWriter.WriteOpen();
                        jsonWriter.WriteProperty("type", "Feature", true);
                        jsonWriter.WriteProperty("name", "Stop", true);
                        jsonWriter.WriteProperty("Shape", Extensions.ToInvariantString(stop.Shape), true);
                        jsonWriter.WritePropertyName("geometry");

                        jsonWriter.WriteOpen();
                        jsonWriter.WriteProperty("type", "Point", true);
                        jsonWriter.WritePropertyName("coordinates");
                        jsonWriter.WriteArrayOpen();
                        jsonWriter.WriteArrayValue(Extensions.ToInvariantString(stop.Coordinate.Longitude));
                        jsonWriter.WriteArrayValue(Extensions.ToInvariantString(stop.Coordinate.Latitude));
                        if (stop.Coordinate.Elevation.HasValue)
                            jsonWriter.WriteArrayValue(Extensions.ToInvariantString(stop.Coordinate.Elevation.Value));
                        jsonWriter.WriteArrayClose();
                        jsonWriter.WriteClose();

                        jsonWriter.WritePropertyName("properties");
                        jsonWriter.WriteOpen();
                        if (stop.Attributes != null)
                        {
                            var attributes = stop.Attributes;
                            if (attributesCallback != null)
                            {
                                attributes = new AttributeCollection(attributes);
                                attributesCallback(attributes);
                            }

                            foreach (var attribute in attributes)
                            {
                                var raw = isRaw != null &&
                                          isRaw(attribute.Key, attribute.Value);
                                jsonWriter.WriteProperty(attribute.Key, attribute.Value, !raw, !raw);
                            }
                        }

                        jsonWriter.WriteClose();

                        jsonWriter.WriteClose();
                    }
            }
        }
    }
}