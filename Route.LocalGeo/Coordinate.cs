using System;
using System.Collections.Generic;
using Route.Navigation.Directions;

namespace Route.LocalGeo
{
    /// <summary>
    ///     Represents a coordinate.
    /// </summary>
    public struct Coordinate
    {
        private const double RadiusOfEarth = 6371000;

        /// <summary>
        ///     Creates a new coordinate.
        /// </summary>
        public Coordinate(float latitude, float longitude)
        {
            Latitude = latitude;
            Longitude = longitude;
            Elevation = null;
        }

        /// <summary>
        ///     Creates a new coordinate.
        /// </summary>
        public Coordinate(float latitude, float longitude, short elevation)
        {
            Latitude = latitude;
            Longitude = longitude;
            Elevation = elevation;
        }

        /// <summary>
        ///     Gets or sets the latitude.
        /// </summary>
        public float Latitude { get; set; }

        /// <summary>
        ///     Gets or sets the longitude.
        /// </summary>
        public float Longitude { get; set; }

        /// <summary>
        ///     Gets or sets the elevation in meter.
        /// </summary>
        public short? Elevation { get; set; }

        /// <summary>
        ///     Offsets this coordinate for a given distance in a given direction.
        /// </summary>
        public Coordinate OffsetWithDirection(float distance, DirectionEnum direction)
        {
            var ratioInRadians = distance / RadiusOfEarth;

            var oldLat = Latitude.ToRadians();
            var oldLon = Longitude.ToRadians();
            var bearing = ((double) (int) direction).ToRadians();

            var newLatitude = Math.Asin(
                Math.Sin(oldLat) *
                Math.Cos(ratioInRadians) +
                Math.Cos(oldLat) *
                Math.Sin(ratioInRadians) *
                Math.Cos(bearing));

            var newLongitude = oldLon + Math.Atan2(
                                   Math.Sin(bearing) *
                                   Math.Sin(ratioInRadians) *
                                   Math.Cos(oldLat),
                                   Math.Cos(ratioInRadians) -
                                   Math.Sin(oldLat) *
                                   Math.Sin(newLatitude));

            var newLat = newLatitude.ToDegrees();
            if (newLat > 180) newLat = newLat - 360;
            var newLon = newLongitude.ToDegrees();
            if (newLon > 180) newLon = newLon - 360;
            return new Coordinate((float) newLat, (float) newLon);
        }

        /// <summary>
        ///     Returns an estimate of the distance between the two given coordinates.
        /// </summary>
        /// <remarks>Accuraccy decreases with distance.</remarks>
        public static float DistanceEstimateInMeter(Coordinate coordinate1, Coordinate coordinate2)
        {
            return DistanceEstimateInMeter(coordinate1.Latitude, coordinate1.Longitude,
                coordinate2.Latitude, coordinate2.Longitude);
        }

        /// <summary>
        ///     Returns an estimate of the distance between the two given coordinates.
        /// </summary>
        /// <remarks>Accuraccy decreases with distance.</remarks>
        public static float DistanceEstimateInMeter(float latitude1, float longitude1, float latitude2,
            float longitude2)
        {
            var lat1Rad = latitude1 / 180d * Math.PI;
            var lon1Rad = longitude1 / 180d * Math.PI;
            var lat2Rad = latitude2 / 180d * Math.PI;
            var lon2Rad = longitude2 / 180d * Math.PI;

            var m = RadiusOfEarth * Math.Acos(Math.Max(-1,
                        Math.Min(1,
                            Math.Sin(lat1Rad) * Math.Sin(lat2Rad) +
                            Math.Cos(lat1Rad) * Math.Cos(lat2Rad) * Math.Cos(lon1Rad - lon2Rad))));

            return (float) m;
        }

        public static float AngleInDegree(Coordinate from, Coordinate to, Coordinate via)
        {
            return AngleInDegree(from.Latitude, from.Longitude,
                to.Latitude, to.Longitude,
                via.Latitude, via.Longitude);
        }

        public static float AngleInDegree(float latitude1, float longitude1, float latitude2,
            float longitude2, float latitude3,
            float longitude3)
        {
            var lat1 = latitude1 - latitude3;
            var lon1 = longitude1 - longitude3;
            var lat2 = latitude2 - latitude3;
            var lon2 = longitude2 - longitude3;

            var r1 = Math.Sqrt(Math.Pow(lat1, 2) + Math.Pow(lon1, 2));
            var r2 = Math.Sqrt(Math.Pow(lat2, 2) + Math.Pow(lon2, 2));
            if (Math.Abs(r1) < 0.000000001 || Math.Abs(r2) < 0.000000001) return 180f;
            var m = Math.Acos((lat1 * lat2 + lon1 * lon2) / (r1 * r2)) * 180d / Math.PI;

            return (float) m;
        }

        /// <summary>
        ///     Returns an estimate of the distance between the given sequence of coordinates.
        /// </summary>
        public static float DistanceEstimateInMeter(List<Coordinate> coordinates)
        {
            var length = 0f;
            for (var i = 1; i < coordinates.Count; i++)
                length += DistanceEstimateInMeter(coordinates[i - 1].Latitude, coordinates[i - 1].Longitude,
                    coordinates[i].Latitude, coordinates[i].Longitude);
            return length;
        }

        /// <summary>
        ///     Returns true if this coordinate is valid.
        /// </summary>
        public bool Valid => Validate(Latitude, Longitude);

        /// <summary>
        ///     Validates the given lat/lon.
        /// </summary>
        /// <param name="lat">The latitude.</param>
        /// <param name="lon">The longitude.</param>
        /// <returns>True if both are in range.</returns>
        public static bool Validate(double lat, double lon)
        {
            if (lat < -90) return false;
            if (lat > 90) return false;
            if (lon < -180) return false;
            if (lon > 180) return false;
            return true;
        }

        /// <summary>
        ///     Offsets this coordinate with a given distance.
        /// </summary>
        public Coordinate OffsetWithDistances(float meter)
        {
            var offsetLat = new Coordinate(
                Latitude + 0.1f, Longitude);
            var offsetLon = new Coordinate(
                Latitude, Longitude + 0.1f);
            var latDistance = DistanceEstimateInMeter(offsetLat, this);
            var lonDistance = DistanceEstimateInMeter(offsetLon, this);

            return new Coordinate(Latitude + meter / latDistance * 0.1f,
                Longitude + meter / lonDistance * 0.1f);
        }

        /// <summary>
        ///     Returns a description of this object.
        /// </summary>
        public override string ToString()
        {
            if (Elevation.HasValue)
                return string.Format("{0},{1}@{2}m", Latitude.ToInvariantString(), Longitude.ToInvariantString(),
                    Elevation.Value.ToInvariantString());
            return string.Format("{0},{1}", Latitude.ToInvariantString(), Longitude.ToInvariantString());
        }
    }
}