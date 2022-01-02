using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Npgsql;
using Placium.Route.Algorithms;
using Placium.Route.Common;
using Route.LocalGeo;

namespace Placium.Route
{
    public class Router
    {
        public Router(RouterDb db)
        {
            Db = db;
        }

        public RouterDb Db { get; }

        public async Task<Route> CalculateAsync(Coordinate source, Coordinate target, string profile)
        {
            var sourceRouterPoint =
                await new ResolveAlgorithm(Db.Guid, Db.ConnectionString, profile, source).DoRunAsync();
            var targetRouterPoint =
                await new ResolveAlgorithm(Db.Guid, Db.ConnectionString, profile, target).DoRunAsync();
            List<long> path = null;
            if (sourceRouterPoint.EdgeId != targetRouterPoint.EdgeId ||
                new[] {1, 4}.Contains(sourceRouterPoint.Direction) &&
                sourceRouterPoint.Offset > targetRouterPoint.Offset ||
                new[] {2, 5}.Contains(sourceRouterPoint.Direction) &&
                sourceRouterPoint.Offset < targetRouterPoint.Offset)
            {
                path = await new InMemoryBidirectionalAStar(Db.Guid, Db.ConnectionString, "motorcar", profile,
                        sourceRouterPoint, targetRouterPoint)
                    .DoRunAsync();
                path.Insert(0, sourceRouterPoint.EdgeId);
                path.Add(targetRouterPoint.EdgeId);
            }
            else
            {
                path = new List<long> {sourceRouterPoint.EdgeId};
            }


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
                command.Parameters.AddWithValue("ids", path.ToArray());
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

            if (path.Count == 1)
            {
                var item = dictionary[path[0]];
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
                else
                    shape.AddRange(item.Coordinates.Reverse().Take(item.Coordinates.Length - targetRouterPoint.Offset)
                        .Skip(item.Coordinates.Length - sourceRouterPoint.Offset));
                shape.Add(targetRouterPoint.Coordinate);
            }
            else
            {
                var item = dictionary[path[0]];
                shapeMeta.Add(new Route.Meta
                {
                    Shape = shape.Count,
                    Attributes = item.Tags.ToAttributes()
                });
                shape.Add(sourceRouterPoint.Coordinate);
                if (sourceRouterPoint.ToNode == item.FromNode || sourceRouterPoint.FromNode == item.FromNode)
                    shape.AddRange(item.Coordinates.Skip(sourceRouterPoint.Offset));
                else
                    shape.AddRange(item.Coordinates.Reverse()
                        .Skip(item.Coordinates.Length - sourceRouterPoint.Offset));

                for (var i = 1; i < path.Count - 1; i++)
                {
                    var prev = dictionary[path[i - 1]];
                    item = dictionary[path[i]];
                    if (item.Coordinates.Length > 1)
                    {
                        shapeMeta.Add(new Route.Meta
                        {
                            Shape = shape.Count,
                            Attributes = item.Tags.ToAttributes()
                        });
                        if (prev.ToNode == item.FromNode || prev.FromNode == item.FromNode)
                            shape.AddRange(item.Coordinates.Skip(1));
                        else shape.AddRange(item.Coordinates.Reverse().Skip(1));
                    }
                }

                item = dictionary[path[path.Count - 1]];

                shapeMeta.Add(new Route.Meta
                {
                    Shape = shape.Count,
                    Attributes = item.Tags.ToAttributes()
                });

                if (targetRouterPoint.ToNode == item.FromNode || targetRouterPoint.FromNode == item.FromNode)
                    shape.AddRange(item.Coordinates.Take(targetRouterPoint.Offset).Skip(1));
                else
                    shape.AddRange(item.Coordinates.Reverse().Take(item.Coordinates.Length - targetRouterPoint.Offset)
                        .Skip(1));
                shape.Add(targetRouterPoint.Coordinate);
            }

            // set stops.
            var stops = new Route.Stop[]
            {
                new Route.Stop()
                {
                    Shape = 0,
                    Coordinate = sourceRouterPoint.Coordinate
                },
                new Route.Stop()
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