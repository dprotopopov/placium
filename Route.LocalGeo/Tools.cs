﻿using System;

namespace Route.LocalGeo
{
    /// <summary>
    ///     Some tools for angles and other geo-stuff.
    /// </summary>
    public static class Tools
    {
        /// <summary>
        ///     Converts degrees to radians.
        /// </summary>
        public static double ToRadians(this double degrees)
        {
            return degrees / 180d * Math.PI;
        }

        /// <summary>
        ///     Converts degrees to radians.
        /// </summary>
        public static double ToRadians(this float degrees)
        {
            return degrees / 180d * Math.PI;
        }

        /// <summary>
        ///     Converts radians to degrees.
        /// </summary>
        public static double ToDegrees(this double radians)
        {
            return radians / Math.PI * 180d;
        }

        /// <summary>
        ///     Converts radians to degrees.
        /// </summary>
        public static double ToDegrees(this float radians)
        {
            return radians / Math.PI * 180d;
        }

        /// <summary>
        ///     Normalizes this angle to the range of [0-360[.
        /// </summary>
        public static double NormalizeDegrees(this double degrees)
        {
            if (degrees >= 360)
            {
                var count = Math.Floor(degrees / 360.0);
                degrees -= 360.0 * count;
            }
            else if (degrees < 0)
            {
                var count = Math.Floor(-degrees / 360.0) + 1;
                degrees += 360.0 * count;
            }

            return degrees;
        }

        /// <summary>
        ///     The smallest difference in degrees between the two angles.
        /// </summary>
        /// <param name="angle1">The first angle, assumed normalized in [0, 360[.</param>
        /// <param name="angle2">The second angle, assumed normalized in [0, 360[.</param>
        /// <returns></returns>
        public static double SmallestDiffDegrees(this double angle1, double angle2)
        {
            if (angle1 < 0 || angle1 > 360.0)
                throw new ArgumentOutOfRangeException(nameof(angle1), "Expected in range [0, 360[.");
            if (angle2 < 0 || angle2 > 360.0)
                throw new ArgumentOutOfRangeException(nameof(angle2), "Expected in range [0, 360[.");

            var diff = angle1 - angle2;
            if (diff > 180)
                diff -= 360;
            else if (diff < -180)
                diff += 360;
            else if (diff == -180) diff = 180;
            return diff;
        }
    }
}