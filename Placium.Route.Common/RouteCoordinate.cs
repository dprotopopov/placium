using NpgsqlTypes;

namespace Placium.Route.Common;

public class RouteCoordinate
{
    [PgName("latitude")] public float Latitude { get; set; }

    [PgName("longitude")] public float Longitude { get; set; }
}