using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Npgsql;
using Placium.Route.Algorithms;
using Placium.Route.Common;
using Placium.Route.Profiles;
using Route.LocalGeo;

namespace Placium.Route
{
    public class Router
    {
        public Router(RouterDb db, string profile, float maxFactor=1f, float minFactor = 1f)
        {
            Db = db;
            Profile = profile;
            MaxFactor = maxFactor;
            MinFactor = minFactor;
            ResolveRouterPointAlgorithm = new ResolveRouterPointAlgorithm(Db.Guid, Db.ConnectionString, Profile);
            PathFinderAlgorithm =
                new InMemoryBidirectionalAStar(Db.Guid, Db.ConnectionString, "motorcar", Profile, MinFactor);
        }

        public RouterDb Db { get; }
        public string Profile { get; }
        public float MinFactor { get; }
        public float MaxFactor { get; }
        public ResolveRouterPointAlgorithm ResolveRouterPointAlgorithm { get; }
        public BasePathFinderAlgorithm PathFinderAlgorithm { get; }

        public async Task<Route> CalculateAsync(Coordinate source, Coordinate target)
        {
            var sourceRouterPoint =
                await ResolveRouterPointAlgorithm.ResolveRouterPointAsync(source);
            var targetRouterPoint =
                await ResolveRouterPointAlgorithm.ResolveRouterPointAsync(target);

            const float maxDistance = 100000f;

            PathFinderResult result = null;
            if (sourceRouterPoint.EdgeId != targetRouterPoint.EdgeId ||
                new[] {1, 4}.Contains(sourceRouterPoint.Direction) &&
                sourceRouterPoint.Offset > targetRouterPoint.Offset ||
                new[] {2, 5}.Contains(sourceRouterPoint.Direction) &&
                sourceRouterPoint.Offset < targetRouterPoint.Offset)
            {
                var maxWeight = MaxFactor* maxDistance;
                result = await PathFinderAlgorithm.FindPathAsync(sourceRouterPoint, targetRouterPoint, maxWeight);
                result.Edges.Insert(0, sourceRouterPoint.EdgeId);
                result.Edges.Add(targetRouterPoint.EdgeId);
            }
            else
            {
                result = new PathFinderResult
                {
                    Edges = new List<long> {sourceRouterPoint.EdgeId}
                };
            }

            var edges = result.Edges;

            var list = new List<EdgeItem>();
            using var connection = new NpgsqlConnection(Db.ConnectionString);
            await connection.OpenAsync();
            connection.TypeMapper.MapComposite<RouteCoordinate>("coordinate");

            using (var command =
                new NpgsqlCommand(
                    @"SELECT id,from_node,to_node,coordinates,tags FROM edge WHERE guid=@guid AND id=ANY(@ids)",
                    connection))
            {
                command.Parameters.AddWithValue("guid", Db.Guid);
                command.Parameters.AddWithValue("ids", edges.ToArray());
                command.Prepare();
                using var reader = await command.ExecuteReaderAsync();
                while (reader.Read())
                {
                    var edgeId = reader.GetInt64(0);
                    var fromNode = reader.GetInt64(1);
                    var toNode = reader.GetInt64(2);
                    var routeCoordinates = (RouteCoordinate[]) reader.GetValue(3);
                    var tags = (Dictionary<string, string>) reader.GetValue(4);
                    var coordinates = routeCoordinates.Select(coord => new Coordinate(coord.Latitude, coord.Longitude))
                        .ToArray();
                    list.Add(new EdgeItem
                    {
                        Id = edgeId,
                        FromNode = fromNode,
                        ToNode = toNode,
                        Coordinates = coordinates,
                        Tags = tags
                    });
                }
            }

            var dictionary = list.ToDictionary(x => x.Id, x => x);

            var shape = new List<Coordinate>(list.Select(x => x.Coordinates.Length).Sum());
            var shapeMeta = new List<Route.Meta>(list.Count);

            if (edges.Count == 1)
            {
                var item = dictionary[edges[0]];
                shapeMeta.Add(new Route.Meta
                {
                    Shape = shape.Count,
                    Attributes = item.Tags.ToAttributes()
                });
                shape.Add(sourceRouterPoint.Coordinate);
                if (sourceRouterPoint.ToNode == targetRouterPoint.FromNode ||
                    sourceRouterPoint.FromNode == targetRouterPoint.FromNode)
                    shape.AddRange(item.Coordinates.Take(targetRouterPoint.Offset)
                        .Skip(sourceRouterPoint.Offset));
                else if (sourceRouterPoint.ToNode == targetRouterPoint.ToNode ||
                         sourceRouterPoint.FromNode == targetRouterPoint.ToNode)
                    shape.AddRange(item.Coordinates.Reverse().Take(item.Coordinates.Length - targetRouterPoint.Offset)
                        .Skip(item.Coordinates.Length - sourceRouterPoint.Offset));
                else if (Coordinate.DistanceEstimateInMeter(sourceRouterPoint.Coordinate, item.Coordinates.First()) <=
                         Coordinate.DistanceEstimateInMeter(sourceRouterPoint.Coordinate, item.Coordinates.Last()))
                    shape.AddRange(item.Coordinates.Take(targetRouterPoint.Offset)
                        .Skip(sourceRouterPoint.Offset));
                else
                    shape.AddRange(item.Coordinates.Reverse().Take(item.Coordinates.Length - targetRouterPoint.Offset)
                        .Skip(item.Coordinates.Length - sourceRouterPoint.Offset));
                shape.Add(targetRouterPoint.Coordinate);
            }
            else
            {
                var item = dictionary[edges[0]];
                var item1 = dictionary[edges[1]];

                shapeMeta.Add(new Route.Meta
                {
                    Shape = shape.Count,
                    Attributes = item.Tags.ToAttributes()
                });
                shape.Add(sourceRouterPoint.Coordinate);
                if (item.ToNode == item1.FromNode || item.ToNode == item1.ToNode)
                    shape.AddRange(item.Coordinates.Skip(sourceRouterPoint.Offset));
                else if (item.FromNode == item1.FromNode || item.FromNode == item1.ToNode)
                    shape.AddRange(sourceRouterPoint.Coordinates.Reverse()
                        .Skip(item.Coordinates.Length - sourceRouterPoint.Offset));
                else if (Coordinate.DistanceEstimateInMeter(sourceRouterPoint.Coordinate, item.Coordinates.First()) <=
                         Coordinate.DistanceEstimateInMeter(sourceRouterPoint.Coordinate, item.Coordinates.Last()))
                    shape.AddRange(item.Coordinates.Skip(sourceRouterPoint.Offset));
                else
                    shape.AddRange(item.Coordinates.Reverse()
                        .Skip(item.Coordinates.Length - sourceRouterPoint.Offset));


                for (var i = 1; i < edges.Count - 1; i++)
                {
                    var prev = dictionary[edges[i - 1]];
                    item = dictionary[edges[i]];
                    if (item.Coordinates.Length > 1)
                    {
                        shapeMeta.Add(new Route.Meta
                        {
                            Shape = shape.Count,
                            Attributes = item.Tags.ToAttributes()
                        });
                        if (prev.ToNode == item.FromNode || prev.FromNode == item.FromNode)
                            shape.AddRange(item.Coordinates.Skip(1));
                        else if (prev.ToNode == item.ToNode || prev.FromNode == item.ToNode)
                            shape.AddRange(item.Coordinates.Reverse().Skip(1));
                        else if (Math.Min(
                                     Coordinate.DistanceEstimateInMeter(prev.Coordinates.First(),
                                         item.Coordinates.First()),
                                     Coordinate.DistanceEstimateInMeter(prev.Coordinates.Last(),
                                         item.Coordinates.First())) <=
                                 Math.Min(
                                     Coordinate.DistanceEstimateInMeter(prev.Coordinates.First(),
                                         item.Coordinates.Last()),
                                     Coordinate.DistanceEstimateInMeter(prev.Coordinates.Last(),
                                         item.Coordinates.Last())))
                            shape.AddRange(item.Coordinates);
                        else
                            shape.AddRange(item.Coordinates.Reverse());
                    }
                }

                item = dictionary[edges[edges.Count - 1]];
                item1 = dictionary[edges[edges.Count - 2]];

                shapeMeta.Add(new Route.Meta
                {
                    Shape = shape.Count,
                    Attributes = item.Tags.ToAttributes()
                });

                if (item.FromNode == item1.FromNode || item.FromNode == item1.ToNode)
                    shape.AddRange(item.Coordinates.Take(targetRouterPoint.Offset).Skip(1));
                else if (item.ToNode == item1.FromNode || item.ToNode == item1.ToNode)
                    shape.AddRange(item.Coordinates.Reverse()
                        .Take(item.Coordinates.Length - targetRouterPoint.Offset)
                        .Skip(1));
                else if (Coordinate.DistanceEstimateInMeter(targetRouterPoint.Coordinate, item.Coordinates.First()) <=
                         Coordinate.DistanceEstimateInMeter(targetRouterPoint.Coordinate, item.Coordinates.Last()))
                    shape.AddRange(item.Coordinates.Skip(sourceRouterPoint.Offset));
                else
                    shape.AddRange(item.Coordinates.Reverse().Skip(item.Coordinates.Length - sourceRouterPoint.Offset));
                shape.AddRange(item.Coordinates);
                shape.Add(targetRouterPoint.Coordinate);
            }

            // set stops.
            var stops = new[]
            {
                new Route.Stop
                {
                    Shape = 0,
                    Coordinate = sourceRouterPoint.Coordinate
                },
                new Route.Stop
                {
                    Shape = shape.Count - 1,
                    Coordinate = targetRouterPoint.Coordinate
                }
            };


            return new Route
            {
                Shape = shape.ToArray(),
                ShapeMeta = shapeMeta.ToArray(),
                Stops = stops
            };
        }

        public class EdgeItem
        {
            public long Id { get; set; }
            public Coordinate[] Coordinates { get; set; }
            public Dictionary<string, string> Tags { get; set; }
            public long FromNode { get; set; }
            public long ToNode { get; set; }
        }
    }
}