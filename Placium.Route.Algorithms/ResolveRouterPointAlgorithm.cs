using System;
using System.Linq;
using System.Threading.Tasks;
using Npgsql;
using Placium.Route.Common;
using Route.LocalGeo;

namespace Placium.Route.Algorithms
{
    public class ResolveRouterPointAlgorithm : BaseResolveRouterPointAlgorithm
    {
        public ResolveRouterPointAlgorithm(Guid guid, string connectionString, string profile) : base(guid,
            connectionString, profile)
        {
        }


        public override async Task<RouterPoint> ResolveRouterPointAsync(Coordinate coordinate)
        {
            using var connection = new NpgsqlConnection(ConnectionString);
            await connection.OpenAsync();
            connection.ReloadTypes();
            connection.TypeMapper.MapComposite<RouteCoordinate>("coordinate");
            using var command = new NpgsqlCommand(
                @"SELECT id,from_node,to_node,coordinates,weight,direction,ST_X(point)::real,ST_Y(point)::real 
                    FROM (SELECT id,from_node,to_node,coordinates,(weight->@profile)::real AS weight,(direction->@profile)::smallint AS direction,
                    ST_ClosestPoint(location, ST_SetSRID( ST_Point( @lon, @lat ), 4326 )::geometry) AS point
                    FROM edge WHERE guid=@guid AND weight?@profile AND (direction->@profile)::smallint=ANY(ARRAY[0,1,2])
                    ORDER BY location <-> ST_SetSRID( ST_Point( @lon, @lat ), 4326 )::geometry
                    LIMIT 1) q", connection);
            command.Parameters.AddWithValue("lat", coordinate.Latitude);
            command.Parameters.AddWithValue("lon", coordinate.Longitude);
            command.Parameters.AddWithValue("profile", Profile);
            command.Parameters.AddWithValue("guid", Guid);
            command.Prepare();

            using var reader = await command.ExecuteReaderAsync();
            if (!reader.Read()) throw new NullReferenceException();
            var edgeId = reader.GetInt64(0);
            var fromNode = reader.GetInt64(1);
            var toNode = reader.GetInt64(2);
            var routeCoordinates = (RouteCoordinate[]) reader.GetValue(3);
            var weight = reader.GetFloat(4);
            var direction = reader.GetInt16(5);
            var longitude = reader.GetFloat(6);
            var latitude = reader.GetFloat(7);
            var coord1 = new Coordinate(latitude, longitude);
            var coordinates = routeCoordinates.Select(coord => new Coordinate(coord.Latitude, coord.Longitude))
                .ToArray();
            var offset = 0;
            for (var i = 1; i < coordinates.Length; i++)
                if (Math.Abs(Coordinate.DistanceEstimateInMeter(coord1, coordinates[i - 1]) +
                             Coordinate.DistanceEstimateInMeter(coord1, coordinates[i]) -
                             Coordinate.DistanceEstimateInMeter(coordinates[i - 1], coordinates[i])) < 1f)
                {
                    offset = i;
                    break;
                }

            return new RouterPoint
            {
                EdgeId = edgeId,
                Coordinate = coord1,
                Offset = offset,
                FromNode = fromNode,
                ToNode = toNode,
                Coordinates = coordinates,
                Weight = weight,
                Direction = direction
            };
        }
    }
}