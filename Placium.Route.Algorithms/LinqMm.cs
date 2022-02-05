using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
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
            var stopWatch = new Stopwatch();

            stopWatch.Start();

            var minWeight = MinFactor * 1;
            var size = 0.01f;

            using var connection2 = new NpgsqlConnection(ConnectionString);
            await connection2.OpenAsync();


            var tempPrefetched = new ConcurrentDictionary<long, TempPrefetched>();
            var tempNode = new ConcurrentDictionary<long, TempNode>();
            var tempEdge = new ConcurrentDictionary<long, TempEdge>();
            var tempDijkstra1 = new ConcurrentDictionary<long, TempDijkstra>();
            var tempDijkstra2 = new ConcurrentDictionary<long, TempDijkstra>();
            var tempRestriction = new ConcurrentDictionary<long, TempRestriction>();


            using var commandSelectFromNode =
                new NpgsqlCommand(string.Join(";",
                        @"SELECT id,latitude,longitude FROM node WHERE id=@node",
                        @"WITH cte AS (SELECT id,latitude,longitude FROM node WHERE id=@node AND guid=@guid),
                        cte2 AS (SELECT n2.id FROM node n2 JOIN cte n1
                        ON n2.latitude<=n1.latitude+@size 
                        AND n2.longitude<=n1.longitude+@size 
                        AND n2.latitude>=n1.latitude-@size 
                        AND n2.longitude>=n1.longitude-@size
                        WHERE n2.guid=@guid)
                    SELECT n.id,n.latitude,n.longitude
                    FROM node n JOIN edge e ON n.id=e.from_node JOIN cte2 n2 ON n2.id=e.to_node
                    WHERE n.guid=@guid AND e.guid=@guid
                    UNION ALL SELECT n.id,n.latitude,n.longitude
                    FROM node n JOIN edge e ON n.id=e.to_node JOIN cte2 n2 ON n2.id=e.from_node
                    WHERE n.guid=@guid AND e.guid=@guid",
                        @"WITH cte AS (SELECT id,latitude,longitude FROM node WHERE id=@node AND guid=@guid),
                        cte2 AS (SELECT n2.id FROM node n2 JOIN cte n1
                        ON n2.latitude<=n1.latitude+@size 
                        AND n2.longitude<=n1.longitude+@size 
                        AND n2.latitude>=n1.latitude-@size 
                        AND n2.longitude>=n1.longitude-@size
                        WHERE n2.guid=@guid)
                    SELECT e.id,e.from_node,e.to_node,
                    GREATEST((weight->@profile)::real,@minWeight),(direction->@profile)::smallint
                    FROM edge e JOIN cte2 n2 ON e.from_node=n2.id OR e.to_node=n2.id
                    WHERE weight?@profile AND direction?@profile AND e.guid=@guid",
                        @"WITH cte AS (SELECT id,latitude,longitude FROM node WHERE id=@node AND guid=@guid),
                        cte2 AS (SELECT n2.id FROM node n2 JOIN cte n1
                        ON n2.latitude<=n1.latitude+@size 
                        AND n2.longitude<=n1.longitude+@size 
                        AND n2.latitude>=n1.latitude-@size 
                        AND n2.longitude>=n1.longitude-@size
                        WHERE n2.guid=@guid)
                    SELECT r.id,r.from_edge,r.to_edge,r.via_node FROM restriction r 
                    JOIN edge e ON r.from_edge=e.id OR r.to_edge=e.id JOIN cte2 n2 ON e.from_node=n2.id OR e.to_node=n2.id
                    WHERE r.vehicle_type=@vehicleType AND r.guid=@guid AND e.guid=@guid
                    UNION ALL SELECT r.id,r.from_edge,r.to_edge,r.via_node FROM restriction r 
                    JOIN cte2 n2 ON r.via_node=n2.id
                    WHERE r.vehicle_type=@vehicleType AND r.guid=@guid"),
                    connection2);


            commandSelectFromNode.Parameters.AddWithValue("minWeight", minWeight);
            commandSelectFromNode.Parameters.AddWithValue("vehicleType", VehicleType);
            commandSelectFromNode.Parameters.AddWithValue("profile", Profile);
            commandSelectFromNode.Parameters.AddWithValue("guid", Guid);
            commandSelectFromNode.Parameters.AddWithValue("size", size);
            commandSelectFromNode.Parameters.Add("node", NpgsqlDbType.Bigint);
            commandSelectFromNode.Prepare();

            void LoadEdgesAndNodes(long node)
            {
                if (tempNode.TryGetValue(node, out var item1) && tempPrefetched.Any(x =>
                        x.Value.Latitude <= item1.Latitude + size &&
                        x.Value.Latitude >= item1.Latitude - size &&
                        x.Value.Longitude <= item1.Longitude + size &&
                        x.Value.Longitude >= item1.Longitude - size))
                    return;

                commandSelectFromNode.Parameters["node"].Value = node;

                using (var reader = commandSelectFromNode.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        var item = new TempPrefetched();
                        item.Id = reader.GetInt64(0);
                        item.Latitude = reader.GetFloat(1);
                        item.Longitude = reader.GetFloat(2);
                        tempPrefetched.TryAdd(item.Id, item);
                    }

                    reader.NextResult();

                    while (reader.Read())
                    {
                        var item = new TempNode();
                        item.Id = reader.GetInt64(0);
                        var latitude = reader.GetFloat(1);
                        var longitude = reader.GetFloat(2);
                        item.Latitude = latitude;
                        item.Longitude = longitude;
                        item.FromWeight = MinFactor * Coordinate.DistanceEstimateInMeter(source.Coordinate,
                                              new Coordinate(latitude, longitude));
                        item.ToWeight = MinFactor * Coordinate.DistanceEstimateInMeter(target.Coordinate,
                                            new Coordinate(latitude, longitude));
                        tempNode.TryAdd(item.Id, item);
                    }

                    reader.NextResult();

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

                    reader.NextResult();

                    while (reader.Read())
                    {
                        var restriction = new TempRestriction();
                        restriction.Id = reader.GetInt64(0);
                        restriction.FromEdge = reader.GetInt64(1);
                        restriction.ToEdge = reader.GetInt64(2);
                        restriction.ViaNode = reader.GetInt64(3);
                        tempRestriction.TryAdd(restriction.Id, restriction);
                    }
                }
            }


            if (new[] {0, 1, 3, 4}.Contains(source.Direction))
            {
                LoadEdgesAndNodes(source.ToNode);
                var coords = source.Coordinates.Last();
                var item = new TempDijkstra();
                item.Node = source.ToNode;
                item.F = MinFactor * Coordinate.DistanceEstimateInMeter(coords, target.Coordinate);
                item.InQueue = true;
                tempDijkstra1.TryAdd(item.Node, item);
            }

            if (new[] {0, 2, 3, 5}.Contains(source.Direction))
            {
                LoadEdgesAndNodes(source.FromNode);
                var coords = source.Coordinates.First();
                var item = new TempDijkstra();
                item.Node = source.FromNode;
                item.F = MinFactor * Coordinate.DistanceEstimateInMeter(coords, target.Coordinate);
                item.InQueue = true;
                tempDijkstra1.TryAdd(item.Node, item);
            }

            if (new[] {0, 1, 3, 4}.Contains(target.Direction))
            {
                LoadEdgesAndNodes(target.FromNode);
                var coords = target.Coordinates.First();
                var item = new TempDijkstra();
                item.Node = target.FromNode;
                item.F = MinFactor * Coordinate.DistanceEstimateInMeter(coords, source.Coordinate);
                item.InQueue = true;
                tempDijkstra2.TryAdd(item.Node, item);
            }

            if (new[] {0, 2, 3, 5}.Contains(target.Direction))
            {
                LoadEdgesAndNodes(target.ToNode);
                var coords = target.Coordinates.Last();
                var item = new TempDijkstra();
                item.Node = target.ToNode;
                item.F = MinFactor * Coordinate.DistanceEstimateInMeter(coords, source.Coordinate);
                item.InQueue = true;
                tempDijkstra2.TryAdd(item.Node, item);
            }

            var steps = 0L;
            var node = 0L;
            float? weight = null;

            for (; ; steps++)
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
                    where !(from r in tempRestriction
                        where r.Value.ViaNode == t1.Key
                              && r.Value.FromEdge == t1.Value.Edge
                              && r.Value.ToEdge == t2.Value.Edge
                        select 0).Any()
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

                var r0 = q.FirstOrDefault();
                if (r0 != null)
                    if (r0.Weight <= maxWeight)
                    {
                        weight = maxWeight = r0.Weight;
                        node = r0.Node;
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
                        where e.Value.FromNode == node1 && new[] {0, 1, 3, 4}.Contains(e.Value.Direction)
                        let n = tempNode[e.Value.ToNode]
                        let t = tempDijkstra1[e.Value.FromNode]
                        where !(from r in tempRestriction
                            where r.Value.ViaNode == node1
                                  && r.Value.FromEdge == t.Edge
                                  && r.Value.ToEdge == e.Value.Id
                            select 0).Any()
                        select new TempDijkstra
                        {
                            Node = n.Id,
                            Edge = e.Value.Id,
                            F = n.ToWeight + t.G + e.Value.Weight,
                            G = t.G + e.Value.Weight,
                            InQueue = true
                        }).Union(from e in tempEdge.AsParallel()
                        where e.Value.ToNode == node1 && new[] {0, 2, 3, 5}.Contains(e.Value.Direction)
                        let n = tempNode[e.Value.FromNode]
                        let t = tempDijkstra1[e.Value.ToNode]
                        where !(from r in tempRestriction
                            where r.Value.ViaNode == node1
                                  && r.Value.FromEdge == t.Edge
                                  && r.Value.ToEdge == e.Value.Id
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
                        where e.Value.ToNode == node2 && new[] {0, 1, 3, 4}.Contains(e.Value.Direction)
                        let n = tempNode[e.Value.FromNode]
                        let t = tempDijkstra2[e.Value.ToNode]
                        where !(from r in tempRestriction
                            where r.Value.ViaNode == node2
                                  && r.Value.ToEdge == t.Edge
                                  && r.Value.FromEdge == e.Value.Id
                            select 0).Any()
                        select new TempDijkstra
                        {
                            Node = n.Id,
                            Edge = e.Value.Id,
                            F = n.FromWeight + t.G + e.Value.Weight,
                            G = t.G + e.Value.Weight,
                            InQueue = true
                        }).Union(from e in tempEdge.AsParallel()
                        where e.Value.FromNode == node2 && new[] {0, 2, 3, 5}.Contains(e.Value.Direction)
                        let n = tempNode[e.Value.ToNode]
                        let t = tempDijkstra2[e.Value.FromNode]
                        where !(from r in tempRestriction
                            where r.Value.ViaNode == node2
                                  && r.Value.ToEdge == t.Edge
                                  && r.Value.FromEdge == e.Value.Id
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

            stopWatch.Stop();

            Console.WriteLine($"{nameof(LinqMm)} steps={steps} ElapsedMilliseconds={stopWatch.ElapsedMilliseconds}");

            return new PathFinderResult
            {
                Edges = list,
                Weight = weight
            };
        }

        public class TempPrefetched
        {
            public long Id { get; set; }
            public float Latitude { get; set; }
            public float Longitude { get; set; }
        }

        public class TempNode
        {
            public long Id { get; set; }
            public float Latitude { get; set; }
            public float Longitude { get; set; }
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
            public long FromEdge { get; set; }
            public long ToEdge { get; set; }
            public long ViaNode { get; set; }
        }
    }
}