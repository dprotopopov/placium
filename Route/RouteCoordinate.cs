using NpgsqlTypes;

namespace Route
{
    public class RouteCoordinate
    {
        [PgName("latitude")] public double Latitude { get; set; }
        
        [PgName("longitude")] public double Longitude { get; set; }
    }
}