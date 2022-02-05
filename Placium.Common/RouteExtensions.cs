using System.Globalization;
using Route.LocalGeo;

namespace Placium.Common;

public static class RouteExtensions
{
    public static Coordinate ToCoordinate(this string coords)
    {
        var arr = coords.Split(",");
        var latitude = float.Parse(arr[0].Trim(), NumberStyles.Any, CultureInfo.InvariantCulture);
        var longitude = float.Parse(arr[1].Trim(), NumberStyles.Any, CultureInfo.InvariantCulture);
        return new Coordinate(latitude, longitude);
    }

    public static Box ToBox(this string coords)
    {
        var arr = coords.Split(",");
        var lat1 = float.Parse(arr[0].Trim(), NumberStyles.Any, CultureInfo.InvariantCulture);
        var lon1 = float.Parse(arr[1].Trim(), NumberStyles.Any, CultureInfo.InvariantCulture);
        var lat2 = float.Parse(arr[2].Trim(), NumberStyles.Any, CultureInfo.InvariantCulture);
        var lon2 = float.Parse(arr[3].Trim(), NumberStyles.Any, CultureInfo.InvariantCulture);
        return new Box(lat1, lon1, lat2, lon2);
    }
}