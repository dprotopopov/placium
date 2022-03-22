using System;
using System.Globalization;
using Route.LocalGeo;

namespace Placium.Common
{
    public static class RouteExtensions
    {
        public static Coordinate ToCoordinate(this string coords)
        {
            var arr = coords.Split(",");
            if (arr.Length >= 2
                && float.TryParse(arr[0].Trim(), NumberStyles.Any, CultureInfo.InvariantCulture, out var latitude)
                && float.TryParse(arr[1].Trim(), NumberStyles.Any, CultureInfo.InvariantCulture, out var longitude))
                return new Coordinate(latitude, longitude);
            throw new ArgumentException();
        }

        public static Box ToBox(this string coords)
        {
            var arr = coords.Split(",");
            if (arr.Length >= 4
                && float.TryParse(arr[0].Trim(), NumberStyles.Any, CultureInfo.InvariantCulture, out var lat1)
                && float.TryParse(arr[1].Trim(), NumberStyles.Any, CultureInfo.InvariantCulture, out var lon1)
                && float.TryParse(arr[2].Trim(), NumberStyles.Any, CultureInfo.InvariantCulture, out var lat2)
                && float.TryParse(arr[3].Trim(), NumberStyles.Any, CultureInfo.InvariantCulture, out var lon2))
                return new Box(lat1, lon1, lat2, lon2);
            throw new ArgumentException();
        }
    }
}