﻿/*
 *  Licensed to SharpSoftware under one or more contributor
 *  license agreements. See the NOTICE file distributed with this work for 
 *  additional information regarding copyright ownership.
 * 
 *  SharpSoftware licenses this file to you under the Apache License, 
 *  Version 2.0 (the "License"); you may not use this file except in 
 *  compliance with the License. You may obtain a copy of the License at
 * 
 *       http://www.apache.org/licenses/LICENSE-2.0
 * 
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

using System.Collections.Generic;
using NetTopologySuite.Geometries;
using Route.Algorithms.Networks.Analytics.Trees.Models;
using Route.Navigation.Instructions;
using Polygon = Route.LocalGeo.Polygon;

namespace Route.Geo
{
    /// <summary>
    /// Contains general extensions related to GeoAPI/NTS and Itinero.
    /// </summary>
    public static class Extensions
    {
        /// <summary>
        /// Converts the coordinate to a geoapi coordinate.
        /// </summary>
        public static Coordinate ToCoordinate(this global::Route.LocalGeo.Coordinate coordinate)
        {
            return new Coordinate(coordinate.Longitude, coordinate.Latitude);
        }

        /// <summary>
        /// Converts a geoapi coordinate to a coordinate.
        /// </summary>
        public static global::Route.LocalGeo.Coordinate FromCoordinate(this Coordinate coordinate)
        {
            return new global::Route.LocalGeo.Coordinate((float)coordinate.Y, (float)coordinate.X);
        }

        /// <summary>
        /// Converts a list of coordinates to geoapi coordinates.
        /// </summary>
        public static List<Coordinate> ToCoordinates(this List<global::Route.LocalGeo.Coordinate> coordinates)
        {
            if (coordinates == null)
            {
                return null;
            }

            var geoApiCoordinates = new List<Coordinate>(coordinates.Count);
            for (var i = 0; i < coordinates.Count; i++)
            {
                geoApiCoordinates.Add(coordinates[i].ToCoordinate());
            }
            return geoApiCoordinates;
        }

        /// <summary>
        /// Converts a list of coordinates to geoapi coordinates.
        /// </summary>
        public static Coordinate[] ToCoordinatesArray(this List<global::Route.LocalGeo.Coordinate> coordinates)
        {
            if (coordinates == null)
            {
                return null;
            }

            var geoApiCoordinates = new Coordinate[coordinates.Count];
            for (var i = 0; i < coordinates.Count; i++)
            {
                geoApiCoordinates[i] = coordinates[i].ToCoordinate();
            }
            return geoApiCoordinates;
        }

        /// <summary>
        /// Converts a list of coordinates to geoapi coordinates.
        /// </summary>
        public static List<Coordinate> ToCoordinates(this global::Route.LocalGeo.Coordinate[] coordinates)
        {
            if (coordinates == null)
            {
                return null;
            }

            var geoApiCoordinates = new List<Coordinate>(coordinates.Length);
            for (var i = 0; i < coordinates.Length; i++)
            {
                geoApiCoordinates.Add(coordinates[i].ToCoordinate());
            }
            return geoApiCoordinates;
        }

        /// <summary>
        /// Converts an array of coordinates to an array of geoapi coordinates.
        /// </summary>
        public static Coordinate[] ToCoordinatesArray(this global::Route.LocalGeo.Coordinate[] coordinates)
        {
            if (coordinates == null)
            {
                return null;
            }

            var geoApiCoordinates = new Coordinate[coordinates.Length];
            for (var i = 0; i < coordinates.Length; i++)
            {
                geoApiCoordinates[i] = coordinates[i].ToCoordinate();
            }
            return geoApiCoordinates;
        }

        /// <summary>
        /// Converts the given coordinates list to the a linear ring.
        /// </summary>
        public static LinearRing ToLinearRing(this List<global::Route.LocalGeo.Coordinate> coordinates)
        {
            return new LinearRing(coordinates.ToCoordinatesArray());
        }

        /// <summary>
        /// Converts the given coordinates list list to the an array of linear rings.
        /// </summary>
        public static LinearRing[] ToLinearRings(this List<List<global::Route.LocalGeo.Coordinate>> coordinates)
        {
            var rings = new LinearRing[coordinates.Count];
            for(var i = 0; i < rings.Length; i++)
            {
                rings[i] = coordinates[i].ToLinearRing();
            }
            return rings;
        }

        /// <summary>
        /// Converts the given polygon to and NTS polygon.
        /// </summary>
        public static NetTopologySuite.Geometries.Polygon ToPolygon(this Polygon polygon)
        {
            return new NetTopologySuite.Geometries.Polygon(polygon.ExteriorRing.ToLinearRing(),
                polygon.InteriorRings.ToLinearRings());
        }

        /// <summary>
        /// Converts the given polygon enumerable to a feature collection.
        /// </summary>
        public static NetTopologySuite.Features.FeatureCollection ToFeatureCollection(this IEnumerable<Polygon> polygons)
        {
            var featureCollection = new NetTopologySuite.Features.FeatureCollection();
            foreach(var polygon in polygons)
            {
                featureCollection.Add(new NetTopologySuite.Features.Feature(
                    polygon.ToPolygon(), new NetTopologySuite.Features.AttributesTable()));
            }
            return featureCollection;
        }

        /// <summary>
        /// Converts the given treeedge to a linestring.
        /// </summary>
        public static LineString ToLineString(this TreeEdge edge)
        {
            var coordinates = new Coordinate[edge.Shape.Length];
            for(var i = 0; i < coordinates.Length; i++)
            {
                coordinates[i] = new Coordinate(edge.Shape[i][0], edge.Shape[i][1]);
            }
            return new LineString(coordinates);
        }

        /// <summary>
        /// Converts the given tree to a feature collection.
        /// </summary>
        public static NetTopologySuite.Features.FeatureCollection ToFeatureCollection(this Tree tree)
        {
            var featureCollection = new NetTopologySuite.Features.FeatureCollection();
            foreach (var treeEdge in tree.Edges)
            {
                var attributes = new NetTopologySuite.Features.AttributesTable();
                attributes.Add("weight1", treeEdge.Weight1.ToInvariantString());
                attributes.Add("weight2", treeEdge.Weight2.ToInvariantString());
                attributes.Add("edge", treeEdge.EdgeId.ToInvariantString());
                attributes.Add("previous_edge", treeEdge.PreviousEdgeId.ToInvariantString());
                featureCollection.Add(new NetTopologySuite.Features.Feature(
                    treeEdge.ToLineString(), attributes));
            }
            return featureCollection;
        }

        /// <summary>
        /// Converts the instructions to features.
        /// </summary>
        public static NetTopologySuite.Features.FeatureCollection ToFeatures(this IEnumerable<Instruction> instructions, global::Route.Route route)
        {
            var featureCollection = new NetTopologySuite.Features.FeatureCollection();
            foreach (var instruction in instructions)
            {
                var attributes = new NetTopologySuite.Features.AttributesTable();
                attributes.Add("text", instruction.Text);
                attributes.Add("type", instruction.Type);
                var location = route.Shape[instruction.Shape];
                featureCollection.Add(new NetTopologySuite.Features.Feature(
                    new NetTopologySuite.Geometries.Point(location.ToCoordinate()), attributes));
            }
            return featureCollection;
        }

        /// <summary>
        /// Adds features from on collection to another.
        /// </summary>
        public static void Add(this NetTopologySuite.Features.FeatureCollection features, NetTopologySuite.Features.FeatureCollection featuresToAdd)
        {
            foreach(var feature in featuresToAdd)
            {
                features.Add(feature);
            }
        }
    }
}