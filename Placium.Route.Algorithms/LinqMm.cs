using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using Placium.Route.Common;
using Route.LocalGeo;

namespace Placium.Route.Algorithms
{
    public class LinqMm : BasePathFinderAlgorithm
    {
        public LinqMm(Guid guid, string connectionString, string vehicleType, string profile,
            float minFactor, float maxFactor) :
            base(guid,
                connectionString, vehicleType, profile, minFactor, maxFactor)
        {
        }

        public override async Task<PathFinderResult> FindPathAsync(RouterPoint source,
            RouterPoint target, float maxWeight = float.MaxValue)
        {
            var minWeight = MinFactor * 1;

            using var connection2 = new NpgsqlConnection(ConnectionString);
            await connection2.OpenAsync();


            using (var command =
                new NpgsqlCommand(string.Join(";", @"CREATE EXTENSION IF NOT EXISTS postgis WITH SCHEMA public",
                    @"create or replace function distanceInMeters(lat1 real, lon1 real, lat2 real, lon2 real)
                returns real
                language plpgsql
                as
                $$
                    DECLARE
                        dist float = 0;
                        thi1 float;
                        thi2 float;
                        dthi float;
                        lamda float;
                        a float;
                    BEGIN
                        IF lat1 = lat2 AND lon1 = lon2
                            THEN RETURN dist;
                        ELSE
                            thi1 = pi() * lat1 / 180;
                            thi2 = pi() * lat2 / 180;
                            dthi = pi() * (lat2 - lat1) / 180;
                            lamda = pi() * (lon2 - lon1) / 180;
                            a = pow(sin(dthi/2),2) + cos(thi1) * cos(thi2) * pow(sin(lamda/2),2);
                            dist = 2 * 6371000 * atan2(sqrt(a),sqrt(1-a));
                            RETURN dist;
                        END IF;
                    END
                $$"), connection2))
            {
                command.Prepare();
                await command.ExecuteNonQueryAsync();
            }

            var tempNode = new ConcurrentDictionary<long, TempNode>();
            var tempEdge = new ConcurrentDictionary<long, TempEdge>();
            var tempDijkstra1 = new ConcurrentDictionary<long, TempDijkstra>();
            var tempDijkstra2 = new ConcurrentDictionary<long, TempDijkstra>();
            var tempRestriction = new ConcurrentDictionary<long, TempRestriction>();
            var tempRestrictionFromEdge = new ConcurrentDictionary<(long, long), TempRestrictionFromEdge>();
            var tempRestrictionToEdge = new ConcurrentDictionary<(long, long), TempRestrictionToEdge>();
            var tempRestrictionViaNode = new ConcurrentDictionary<(long, long), TempRestrictionViaNode>();


            using var commandSelectFromRestriction =
                new NpgsqlCommand(string.Join(";",
                        @"SELECT r.rid,r.edge FROM restriction_from_edge r JOIN edge e ON r.edge=e.id
                    WHERE (e.from_node=@node OR e.to_node=@node) AND r.vehicle_type=@vehicleType AND r.guid=@guid AND e.guid=@guid",
                        @"SELECT r.rid,r.edge FROM restriction_to_edge r JOIN edge e ON r.edge=e.id
                    WHERE (e.from_node=@node OR e.to_node=@node) AND r.vehicle_type=@vehicleType AND r.guid=@guid AND e.guid=@guid",
                        @"SELECT rid,node FROM restriction_via_node WHERE node=@node AND vehicle_type=@vehicleType AND guid=@guid"),
                    connection2);

            commandSelectFromRestriction.Parameters.Add("node", NpgsqlDbType.Bigint);
            commandSelectFromRestriction.Parameters.AddWithValue("vehicleType", VehicleType);
            commandSelectFromRestriction.Parameters.AddWithValue("guid", Guid);
            commandSelectFromRestriction.Prepare();


            using var commandSelectFromNode =
                new NpgsqlCommand(string.Join(";",
                        @"SELECT id,from_weight,to_weight FROM (SELECT n.id,
                    @factor*distanceInMeters(latitude,longitude,@fromLatitude,@fromLongitude) AS from_weight,
                    @factor*distanceInMeters(latitude,longitude,@toLatitude,@toLongitude) AS to_weight
                    FROM node n JOIN edge e ON n.id=e.from_node WHERE n.guid=@guid AND e.guid=@guid
                    AND e.to_node=@node) q WHERE from_weight+to_weight<=@maxWeight", @"SELECT id,from_weight,to_weight FROM (SELECT n.id,
                    @factor*distanceInMeters(latitude,longitude,@fromLatitude,@fromLongitude) AS from_weight,
                    @factor*distanceInMeters(latitude,longitude,@toLatitude,@toLongitude) AS to_weight
                    FROM node n JOIN edge e ON n.id=e.to_node WHERE n.guid=@guid AND e.guid=@guid
                    AND e.from_node=@node) q WHERE from_weight+to_weight<=@maxWeight", @"SELECT id,from_node,to_node,
                    GREATEST((weight->@profile)::real,@minWeight),(direction->@profile)::smallint
                    FROM edge WHERE weight?@profile AND direction?@profile AND guid=@guid
                    AND (from_node=@node OR to_node=@node)"),
                    connection2);

            commandSelectFromNode.Parameters.AddWithValue("fromLatitude", source.Coordinate.Latitude);
            commandSelectFromNode.Parameters.AddWithValue("fromLongitude", source.Coordinate.Longitude);
            commandSelectFromNode.Parameters.AddWithValue("toLatitude", target.Coordinate.Latitude);
            commandSelectFromNode.Parameters.AddWithValue("toLongitude", target.Coordinate.Longitude);
            commandSelectFromNode.Parameters.AddWithValue("minWeight", minWeight);
            commandSelectFromNode.Parameters.AddWithValue("maxWeight", maxWeight);
            commandSelectFromNode.Parameters.AddWithValue("profile", Profile);
            commandSelectFromNode.Parameters.AddWithValue("factor", MinFactor);
            commandSelectFromNode.Parameters.AddWithValue("guid", Guid);
            commandSelectFromNode.Parameters.Add("node", NpgsqlDbType.Bigint);
            commandSelectFromNode.Prepare();

            void LoadEdgesAndNodes(long node)
            {
                commandSelectFromNode.Parameters["node"].Value = node;
                commandSelectFromRestriction.Parameters["node"].Value = node;

                using (var reader = commandSelectFromNode.ExecuteReader())
                {
                    for (var i = 0; i < 2; i++)
                    {
                        while (reader.Read())
                        {
                            var item = new TempNode();
                            item.Id = reader.GetInt64(0);
                            item.FromWeight = reader.GetFloat(1);
                            item.ToWeight = reader.GetFloat(2);
                            tempNode.TryAdd(item.Id, item);
                        }

                        reader.NextResult();
                    }

                    while (reader.Read())
                    {
                        var item = new TempEdge();
                        item.Id = reader.GetInt64(0);
                        item.FromNode = reader.GetInt64(1);
                        item.ToNode = reader.GetInt64(2);
                        item.Weight = reader.GetFloat(3);
                        item.Direction = reader.GetInt16(4);
                        tempEdge.TryAdd(item.Id, item);
                    }
                }

                using (var reader = commandSelectFromRestriction.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        var restriction = new TempRestriction();
                        restriction.Id = reader.GetInt64(0);
                        tempRestriction.TryAdd(restriction.Id, restriction);
                        var item = new TempRestrictionFromEdge();
                        item.Rid = reader.GetInt64(0);
                        item.Edge = reader.GetInt64(1);
                        tempRestrictionFromEdge.TryAdd((item.Rid, item.Edge), item);
                    }

                    reader.NextResult();

                    while (reader.Read())
                    {
                        var restriction = new TempRestriction();
                        restriction.Id = reader.GetInt64(0);
                        tempRestriction.TryAdd(restriction.Id, restriction);
                        var item = new TempRestrictionToEdge();
                        item.Rid = reader.GetInt64(0);
                        item.Edge = reader.GetInt64(1);
                        tempRestrictionToEdge.TryAdd((item.Rid, item.Edge), item);
                    }

                    reader.NextResult();

                    while (reader.Read())
                    {
                        var restriction = new TempRestriction();
                        restriction.Id = reader.GetInt64(0);
                        tempRestriction.TryAdd(restriction.Id, restriction);
                        var item = new TempRestrictionViaNode();
                        item.Rid = reader.GetInt64(0);
                        item.Node = reader.GetInt64(1);
                        tempRestrictionViaNode.TryAdd((item.Rid, item.Node), item);
                    }
                }
            }


            if (new[] {0, 1, 3, 4}.Contains(source.Direction))
            {
                var coords = source.Coordinates.Last();
                var item = new TempDijkstra();
                item.Node = source.ToNode;
                item.F = MinFactor * Coordinate.DistanceEstimateInMeter(coords, target.Coordinate);
                item.InQueue = true;
                tempDijkstra1.TryAdd(item.Node, item);
            }

            if (new[] {0, 2, 3, 5}.Contains(source.Direction))
            {
                var coords = source.Coordinates.First();
                var item = new TempDijkstra();
                item.Node = source.FromNode;
                item.F = MinFactor * Coordinate.DistanceEstimateInMeter(coords, target.Coordinate);
                item.InQueue = true;
                tempDijkstra1.TryAdd(item.Node, item);
            }

            if (new[] {0, 1, 3, 4}.Contains(target.Direction))
            {
                var coords = target.Coordinates.First();
                var item = new TempDijkstra();
                item.Node = target.FromNode;
                item.F = MinFactor * Coordinate.DistanceEstimateInMeter(coords, source.Coordinate);
                item.InQueue = true;
                tempDijkstra2.TryAdd(item.Node, item);
            }

            if (new[] {0, 2, 3, 5}.Contains(target.Direction))
            {
                var coords = target.Coordinates.Last();
                var item = new TempDijkstra();
                item.Node = target.ToNode;
                item.F = MinFactor * Coordinate.DistanceEstimateInMeter(coords, source.Coordinate);
                item.InQueue = true;
                tempDijkstra2.TryAdd(item.Node, item);
            }

            var node = 0L;
            float? weight = null;

            for (var step = 0L;; step++)
            {
                var node1 = 0L;
                var node2 = 0L;
                var pr1 = maxWeight;
                var pr2 = maxWeight;
                var fmin1 = 0f;
                var fmin2 = 0f;
                var gmin1 = 0f;
                var gmin2 = 0f;

                var q = from t1 in tempDijkstra1
                    join t2 in tempDijkstra2 on t1.Key equals t2.Key
                    let g = t1.Value.G + t2.Value.G
                    orderby g
                    select new {Node = t1.Key, Weight = g};

                var q1 = from t1 in tempDijkstra1
                    where t1.Value.InQueue
                    let pr = Math.Max(t1.Value.F, 2 * t1.Value.G)
                    orderby pr
                    select new {Node = t1.Key, Pr = pr, t1.Value.F, t1.Value.G};
                var q2 = from t2 in tempDijkstra2
                    where t2.Value.InQueue
                    let pr = Math.Max(t2.Value.F, 2 * t2.Value.G)
                    orderby pr
                    select new {Node = t2.Key, Pr = pr, t2.Value.F, t2.Value.G};

                var r = q.FirstOrDefault();
                if (r != null)
                    if (r.Weight <= maxWeight)
                    {
                        weight = maxWeight = r.Weight;
                        node = r.Node;
                    }

                var r1 = q1.FirstOrDefault();
                var r2 = q2.FirstOrDefault();

                if (r1 == null || r2 == null) break;

                node1 = r1.Node;
                pr1 = r1.Pr;
                fmin1 = r1.F;
                gmin1 = r1.G;
                node2 = r2.Node;
                pr2 = r2.Pr;
                fmin2 = r2.F;
                gmin2 = r2.G;

                var c = Math.Min(pr1, pr2);
                if (maxWeight <= new[] {c, fmin1, fmin2, gmin1 + gmin2 + minWeight}.Max()) break;

                if (pr1 < pr2)
                {
                    LoadEdgesAndNodes(node1);

                    tempDijkstra1[node1].InQueue = false;

                    tempDijkstra1.Where(x => x.Value.F > maxWeight).ToList().AsParallel().ForAll(item =>
                        tempDijkstra1.Remove(item.Key, out var value));

                    var s1 = (from e in tempEdge.AsParallel()
                              where e.Value.FromNode == node1 && new[] { 0, 1, 3, 4 }.Contains(e.Value.Direction)
                              let n = tempNode[e.Value.ToNode]
                              let t = tempDijkstra1[e.Value.FromNode]
                              where !(from via in tempRestrictionViaNode
                                      where via.Value.Node == node1
                                            && tempRestrictionFromEdge.ContainsKey((via.Value.Rid, t.Edge))
                                            && tempRestrictionToEdge.ContainsKey((via.Value.Rid, e.Value.Id))
                                      select 0).Any()
                              select new TempDijkstra
                              {
                                  Node = n.Id,
                                  Edge = e.Value.Id,
                                  F = n.ToWeight + t.G + e.Value.Weight,
                                  G = t.G + e.Value.Weight,
                                  InQueue = true
                              }).Union(from e in tempEdge.AsParallel()
                                       where e.Value.ToNode == node1 && new[] { 0, 2, 3, 5 }.Contains(e.Value.Direction)
                                       let n = tempNode[e.Value.FromNode]
                                       let t = tempDijkstra1[e.Value.ToNode]
                                       where !(from via in tempRestrictionViaNode
                                               where via.Value.Node == node1
                                                     && tempRestrictionFromEdge.ContainsKey((via.Value.Rid, t.Edge))
                                                     && tempRestrictionToEdge.ContainsKey((via.Value.Rid, e.Value.Id))
                                               select 0).Any()
                                       select new TempDijkstra
                                       {
                                           Node = n.Id,
                                           Edge = e.Value.Id,
                                           F = n.ToWeight + t.G + e.Value.Weight,
                                           G = t.G + e.Value.Weight,
                                           InQueue = true
                                       });

                    s1.Where(x => x.G <= maxWeight).ToList().AsParallel().ForAll(item =>
                    {
                        if (tempDijkstra1.TryAdd(item.Node, item)) return;
                        var item1 = tempDijkstra1[item.Node];
                        if (item1.G > item.G)
                            tempDijkstra1[item.Node] = item;
                    });
                }
                else
                {
                    LoadEdgesAndNodes(node2);

                    tempDijkstra2[node2].InQueue = false;

                    tempDijkstra2.Where(x => x.Value.F > maxWeight).ToList().AsParallel().ForAll(item =>
                        tempDijkstra2.Remove(item.Key, out var value));

                    var s2 = (from e in tempEdge.AsParallel()
                              where e.Value.ToNode == node2 && new[] { 0, 1, 3, 4 }.Contains(e.Value.Direction)
                              let n = tempNode[e.Value.FromNode]
                              let t = tempDijkstra2[e.Value.ToNode]
                              where !(from via in tempRestrictionViaNode
                                      where via.Value.Node == node2
                                            && tempRestrictionToEdge.ContainsKey((via.Value.Rid, t.Edge))
                                            && tempRestrictionFromEdge.ContainsKey((via.Value.Rid, e.Value.Id))
                                      select 0).Any()
                              select new TempDijkstra
                              {
                                  Node = n.Id,
                                  Edge = e.Value.Id,
                                  F = n.FromWeight + t.G + e.Value.Weight,
                                  G = t.G + e.Value.Weight,
                                  InQueue = true
                              }).Union(from e in tempEdge.AsParallel()
                                       where e.Value.FromNode == node2 && new[] { 0, 2, 3, 5 }.Contains(e.Value.Direction)
                                       let n = tempNode[e.Value.ToNode]
                                       let t = tempDijkstra2[e.Value.FromNode]
                                       where !(from via in tempRestrictionViaNode
                                               where via.Value.Node == node2
                                                     && tempRestrictionToEdge.ContainsKey((via.Value.Rid, t.Edge))
                                                     && tempRestrictionFromEdge.ContainsKey((via.Value.Rid, e.Value.Id))
                                               select 0).Any()
                                       select new TempDijkstra
                                       {
                                           Node = n.Id,
                                           Edge = e.Value.Id,
                                           F = n.FromWeight + t.G + e.Value.Weight,
                                           G = t.G + e.Value.Weight,
                                           InQueue = true
                                       });

                    s2.Where(x => x.G <= maxWeight).ToList().AsParallel().ForAll(item =>
                    {
                        if (tempDijkstra2.TryAdd(item.Node, item)) return;
                        var item1 = tempDijkstra2[item.Node];
                        if (item1.G > item.G)
                            tempDijkstra2[item.Node] = item;
                    });
                }


                if (step % 100 == 0)
                    Console.WriteLine($"{DateTime.Now:O} Step {step} complete" +
                                      $" temp_dijkstra1={tempDijkstra1.Count(x => x.Value.InQueue)}" +
                                      $" temp_dijkstra2={tempDijkstra2.Count(x => x.Value.InQueue)}" +
                                      $" MIN(g1)={tempDijkstra1.Where(x => x.Value.InQueue).Min(x => x.Value.G)}" +
                                      $" MIN(g2)={tempDijkstra2.Where(x => x.Value.InQueue).Min(x => x.Value.G)}");
            }

            Console.WriteLine($"node = {node}");

            var list = new List<long>();

            for (var node1 = node;;)
            {
                var q = (from t in tempDijkstra1
                    where t.Value.Node == node1 && tempEdge.ContainsKey(t.Value.Edge)
                    let e = tempEdge[t.Value.Edge]
                    select (e.Id, e.FromNode == node1 ? e.ToNode : e.FromNode)).ToList();
                if (!q.Any()) break;
                var (key, value) = q.First();
                list.Add(key);
                node1 = value;
            }

            list.Reverse();

            for (var node2 = node;;)
            {
                var q = (from t in tempDijkstra2
                    where t.Value.Node == node2 && tempEdge.ContainsKey(t.Value.Edge)
                    let e = tempEdge[t.Value.Edge]
                    select (e.Id, e.FromNode == node2 ? e.ToNode : e.FromNode)).ToList();
                if (!q.Any()) break;
                var (key, value) = q.First();
                list.Add(key);
                node2 = value;
            }

            return new PathFinderResult
            {
                Edges = list,
                Weight = weight
            };
        }

        public class TempNode
        {
            public long Id { get; set; }
            public float FromWeight { get; set; }
            public float ToWeight { get; set; }
        }

        public class TempEdge
        {
            public long Id { get; set; }
            public long FromNode { get; set; }
            public long ToNode { get; set; }
            public float Weight { get; set; }
            public short Direction { get; set; }
        }

        public class TempDijkstra
        {
            public long Node { get; set; }
            public float F { get; set; }
            public float G { get; set; }
            public long Edge { get; set; }
            public bool InQueue { get; set; }
        }

        public class TempRestriction
        {
            public long Id { get; set; }
        }

        public class TempRestrictionFromEdge
        {
            public long Rid { get; set; }
            public long Edge { get; set; }
        }

        public class TempRestrictionToEdge
        {
            public long Rid { get; set; }
            public long Edge { get; set; }
        }

        public class TempRestrictionViaNode
        {
            public long Rid { get; set; }
            public long Node { get; set; }
        }
    }
}