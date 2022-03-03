/*
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

using System;

namespace Route.LocalGeo
{
    /// <summary>
    ///     Represents a box.
    /// </summary>
    public struct Box
    {
        /// <summary>
        ///     Creates a new box.
        /// </summary>
        public Box(Coordinate coordinate1, Coordinate coordinate2)
            : this(coordinate1.Latitude, coordinate1.Longitude, coordinate2.Latitude, coordinate2.Longitude)
        {
        }

        /// <summary>
        ///     Creates a new box.
        /// </summary>
        public Box(float lat1, float lon1, float lat2, float lon2)
        {
            if (lat1 < lat2)
            {
                MinLat = lat1;
                MaxLat = lat2;
            }
            else
            {
                MinLat = lat2;
                MaxLat = lat1;
            }

            if (lon1 < lon2)
            {
                MinLon = lon1;
                MaxLon = lon2;
            }
            else
            {
                MinLon = lon2;
                MaxLon = lon1;
            }
        }

        /// <summary>
        ///     Gets the minimum latitude.
        /// </summary>
        public float MinLat { get; }

        /// <summary>
        ///     Gets the maximum latitude.
        /// </summary>
        public float MaxLat { get; }

        /// <summary>
        ///     Gets the minimum longitude.
        /// </summary>
        public float MinLon { get; }

        /// <summary>
        ///     Gets the maximum longitude.
        /// </summary>
        public float MaxLon { get; }

        /// <summary>
        ///     Returns true if this box overlaps the given coordinates.
        /// </summary>
        public bool Overlaps(float lat, float lon)
        {
            return MinLat < lat && lat <= MaxLat &&
                   MinLon < lon && lon <= MaxLon;
        }

        /// <summary>
        ///     Returns true if the given box overlaps with this one. Partial overlaps also return true.
        /// </summary>
        /// <param name="box">The other box.</param>
        /// <returns>True if any parts of the two boxes overlap.</returns>
        public bool Overlaps(Box box)
        {
            var thisCenter = Center;
            if (box.Overlaps(thisCenter.Latitude, thisCenter.Latitude)) return true;
            var otherCenter = box.Center;
            if (Overlaps(otherCenter.Latitude, otherCenter.Latitude)) return true;
            return IntersectsPotentially(box.MinLon, box.MinLat, box.MaxLon, box.MaxLat);
        }

        /// <summary>
        ///     Expands this box (if needed) to incluce the given coordinate.
        /// </summary>
        public Box ExpandWith(float lat, float lon)
        {
            if (Overlaps(lat, lon))
                // assume this happens in most cases.
                return this;

            return new Box(Math.Min(MinLat, lat), Math.Min(MinLon, lon),
                Math.Max(MaxLat, lat), Math.Max(MaxLon, lon));
        }

        /// <summary>
        ///     Returns true if the line potentially intersects with this box.
        /// </summary>
        public bool IntersectsPotentially(float longitude1, float latitude1, float longitude2, float latitude2)
        {
            // TODO: auwch, switch longitude and latitude, this is very very bad!
            if (longitude1 > MaxLon && longitude2 > MaxLon) return false;
            if (longitude1 < MinLon && longitude2 < MinLon) return false;
            if (latitude1 > MaxLat && latitude2 > MaxLat) return false;
            if (latitude1 < MinLat && latitude2 < MinLat) return false;
            return true;
        }

        /// <summary>
        ///     Gets the exact center of this box.
        /// </summary>
        public Coordinate Center =>
            new Coordinate
            {
                Latitude = (MaxLat + MinLat) / 2f,
                Longitude = (MinLon + MaxLon) / 2f
            };

        /// <summary>
        ///     Returns a resized version of this box.
        /// </summary>
        public Box Resize(float e)
        {
            return new Box(MinLat - e, MinLon - e, MaxLat + e, MaxLon + e);
        }
    }
}