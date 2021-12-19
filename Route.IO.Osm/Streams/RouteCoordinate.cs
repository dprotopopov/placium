using NpgsqlTypes;

namespace Route.IO.Osm.Streams
{
    public class RouteCoordinate
    {
        [PgName("latitude")] public double Latitude { get; set; }
        
        [PgName("longitude")] public double Longitude { get; set; }
    }
}