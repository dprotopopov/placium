﻿using System;
using System.Linq;
using System.Threading.Tasks;
using Npgsql;
using Placium.Route.Common;
using Route.LocalGeo;

namespace Placium.Route.Algorithms
{
    public class ResolveAlgorithm : BaseDatabaseAlgorithm<RouterPoint>
    {
        public ResolveAlgorithm(Guid guid, string connectionString, string profile, Coordinate coordinate) : base(guid,
            connectionString, profile)
        {
            Coordinate = coordinate;
        }

        public Coordinate Coordinate { get; }

        public override async Task<RouterPoint> DoRunAsync()
        {
            using var connection = new NpgsqlConnection(ConnectionString);
            await connection.OpenAsync();
            connection.TypeMapper.MapComposite<RouteCoordinate>("coordinate");
            using (var command = new NpgsqlCommand(
                @"SELECT id,from_node,to_node,coordinates,direction,ST_X(point),ST_Y(point) 
                    FROM (SELECT id,from_node,to_node,coordinates,(direction->@profile)::integer AS direction,
                    ST_ClosestPoint(location, ST_SetSRID( ST_Point( @lon, @lat ), 4326 )::geometry) AS point
                    FROM edge WHERE guid=@guid AND (direction->@profile)::integer=ANY(ARRAY[0,1,2])
                    ORDER BY location <-> ST_SetSRID( ST_Point( @lon, @lat ), 4326 )::geometry
                    LIMIT 1) q", connection))
            {
                command.Parameters.AddWithValue("lat", Coordinate.Latitude);
                command.Parameters.AddWithValue("lon", Coordinate.Longitude);
                command.Parameters.AddWithValue("profile", Profile);
                command.Parameters.AddWithValue("guid", Guid);
                command.Prepare();

                using (var reader = await command.ExecuteReaderAsync())
                {
                    if (!reader.Read()) throw new NullReferenceException();
                    var edgeId = reader.GetInt64(0);
                    var fromNode = reader.GetInt64(1);
                    var toNode = reader.GetInt64(2);
                    var routeCoordinates = (RouteCoordinate[]) reader.GetValue(3);
                    var direction = reader.GetInt16(4);
                    var longitude = (float) reader.GetDouble(5);
                    var latitude = (float) reader.GetDouble(6);
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
                        Direction = direction
                    };
                }
            }
        }
    }
}