using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using Placium.Route.Common;

namespace Placium.Route.Algorithms
{
    public class AStar : BasePathFinderAlgorithm
    {
        public AStar(Guid guid, string connectionString, string vehicleType, string profile, float minFactor,
            float maxFactor) : base(guid,
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

            await using var connection = new NpgsqlConnection(ConnectionString);
            await connection.OpenAsync();

            await using (var command =
                         new NpgsqlCommand(string.Join(";",
                             @"CREATE EXTENSION IF NOT EXISTS postgis WITH SCHEMA public",
                             @"create or replace function distanceInMeters(lat1 real, lon1 real, lat2 real, lon2 real)
                returns real
                language plpgsql
                as
                $$
                    DECLARE
                        dist float = 0;
                        phi1 float;
                        phi2 float;
                        delta_phi float;
                        lambda float;
                        a float;
                    BEGIN
                        IF lat1 = lat2 AND lon1 = lon2
                            THEN RETURN dist;
                        ELSE
                            phi1 = pi() * lat1 / 180;
                            phi2 = pi() * lat2 / 180;
                            delta_phi = pi() * (lat2 - lat1) / 180;
                            lambda = pi() * (lon2 - lon1) / 180;
                            a = pow(sin(delta_phi/2),2) + cos(phi1) * cos(phi2) * pow(sin(lambda/2),2);
                            dist = 2 * 6371000 * atan2(sqrt(a),sqrt(1-a));
                            RETURN dist;
                        END IF;
                    END
                $$", @"CREATE TEMP TABLE temp_prefetch (
	                id BIGINT PRIMARY KEY NOT NULL, 
	                latitude REAL NOT NULL, 
	                longitude REAL NOT NULL
                )", @"CREATE TEMP TABLE temp_node (
	                id BIGINT PRIMARY KEY NOT NULL, 
	                latitude REAL NOT NULL, 
	                longitude REAL NOT NULL, 
	                from_weight REAL NOT NULL
                )", @"CREATE TEMP TABLE temp_edge (
	                id BIGINT PRIMARY KEY NOT NULL, 
	                from_node BIGINT NOT NULL, 
	                to_node BIGINT NOT NULL, 
	                weight REAL NOT NULL,
                    direction SMALLINT NOT NULL
                )", @"CREATE TEMP TABLE temp_dijkstra (
	                node BIGINT PRIMARY KEY NOT NULL, 
	                weight REAL NOT NULL, 
	                g REAL NOT NULL, 
	                edge BIGINT NULL,
	                in_queue BOOLEAN NOT NULL,
                    FOREIGN KEY(node) REFERENCES temp_node(id),
                    FOREIGN KEY(edge) REFERENCES temp_edge(id)
                )", @"CREATE TEMP TABLE temp_restriction (
	                id BIGINT PRIMARY KEY NOT NULL,
	                from_edge BIGINT NOT NULL,
	                to_edge BIGINT NOT NULL,
	                via_node BIGINT NOT NULL
                )"), connection))
            {
                await command.PrepareAsync();
                await command.ExecuteNonQueryAsync();
            }


            await using (var command =
                         new NpgsqlCommand(string.Join(";",
                                 @"CREATE INDEX temp_prefetch_latitude_idx ON temp_prefetch (latitude)",
                                 @"CREATE INDEX temp_prefetch_longitude_idx ON temp_prefetch (longitude)",
                                 @"CREATE INDEX temp_node_latitude_idx ON temp_node (latitude)",
                                 @"CREATE INDEX temp_node_longitude_idx ON temp_node (longitude)",
                                 @"CREATE INDEX temp_dijkstra_in_queue_idx ON temp_dijkstra (in_queue)",
                                 @"CREATE INDEX temp_dijkstra_weight_idx ON temp_dijkstra (weight)",
                                 @"CREATE INDEX temp_edge_from_node_to_node_idx ON temp_edge (from_node,to_node)",
                                 @"CREATE UNIQUE INDEX temp_restriction_from_edge_to_edge_via_node_idx ON temp_restriction (from_edge,to_edge,via_node)"),
                             connection))
            {
                await command.PrepareAsync();
                await command.ExecuteNonQueryAsync();
            }

            await using var commandBegin =
                new NpgsqlCommand(@"BEGIN", connection);
            await using var commandCommit =
                new NpgsqlCommand(@"COMMIT", connection);

            await commandBegin.PrepareAsync();
            await commandCommit.PrepareAsync();


            await using var commandSelectFromPrefetch = new NpgsqlCommand(
                @"WITH cte AS (SELECT id,latitude,longitude FROM temp_node WHERE id=@node),
                cte1 AS (SELECT p.id FROM temp_prefetch p JOIN cte n ON p.latitude<=n.latitude+@size),
                cte2 AS (SELECT p.id FROM temp_prefetch p JOIN cte n ON p.longitude<=n.longitude+@size),
                cte3 AS (SELECT p.id FROM temp_prefetch p JOIN cte n ON p.latitude>=n.latitude-@size),
                cte4 AS (SELECT p.id FROM temp_prefetch p JOIN cte n ON p.longitude>=n.longitude-@size)
                SELECT EXISTS (SELECT 1 FROM cte1 JOIN cte2 ON cte1.id=cte2.id JOIN cte3 ON cte1.id=cte3.id JOIN cte4 ON cte1.id=cte4.id)",
                connection);

            commandSelectFromPrefetch.Parameters.Add("node", NpgsqlDbType.Bigint);
            commandSelectFromPrefetch.Parameters.AddWithValue("size", size);
            await commandSelectFromPrefetch.PrepareAsync();

            await using var commandSelectFromNode =
                new NpgsqlCommand(string.Join(";",
                        @"INSERT INTO temp_prefetch (id,latitude,longitude)
                    SELECT id,latitude,longitude FROM node WHERE id=@node
                    ON CONFLICT DO NOTHING",
                        @"INSERT INTO temp_node (id,latitude,longitude,from_weight)
                    WITH cte AS (SELECT id,latitude,longitude FROM node WHERE id=@node AND guid=@guid),
                    cte2 AS (SELECT n2.id FROM node n2 JOIN cte n1
                    ON n2.latitude<=n1.latitude+@size 
                    AND n2.longitude<=n1.longitude+@size 
                    AND n2.latitude>=n1.latitude-@size 
                    AND n2.longitude>=n1.longitude-@size
                    WHERE n2.guid=@guid)
                    SELECT n.id,n.latitude,n.longitude,
                    @factor*distanceInMeters(n.latitude,n.longitude,@fromLatitude,@fromLongitude)
                    FROM node n JOIN edge e ON n.id=e.from_node JOIN cte2 n2 ON n2.id=e.to_node
                    WHERE n.guid=@guid AND e.guid=@guid
                    UNION ALL SELECT n.id,n.latitude,n.longitude,
                    @factor*distanceInMeters(n.latitude,n.longitude,@fromLatitude,@fromLongitude)
                    FROM node n JOIN edge e ON n.id=e.to_node JOIN cte2 n2 ON n2.id=e.from_node
                    WHERE n.guid=@guid AND e.guid=@guid
                    ON CONFLICT (id) DO NOTHING",
                        @"INSERT INTO temp_edge (id,from_node,to_node,weight,direction)
                    WITH cte AS (SELECT id,latitude,longitude FROM node WHERE id=@node AND guid=@guid),
                    cte2 AS (SELECT n2.id FROM node n2 JOIN cte n1
                    ON n2.latitude<=n1.latitude+@size 
                    AND n2.longitude<=n1.longitude+@size 
                    AND n2.latitude>=n1.latitude-@size 
                    AND n2.longitude>=n1.longitude-@size
                    WHERE n2.guid=@guid)
                    SELECT e.id,e.from_node,e.to_node,
                    GREATEST((weight->@profile)::real,@minWeight),(direction->@profile)::smallint
                    FROM edge e JOIN cte2 n2 ON e.from_node=n2.id OR e.to_node=n2.id
                    WHERE weight?@profile AND direction?@profile AND e.guid=@guid
                    ON CONFLICT (id) DO NOTHING",
                        @"INSERT INTO temp_restriction(id,from_edge,to_edge,via_node)
                    WITH cte AS (SELECT id,latitude,longitude FROM node WHERE id=@node AND guid=@guid),
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
                    WHERE r.vehicle_type=@vehicleType AND r.guid=@guid
                    ON CONFLICT (from_edge,to_edge,via_node) DO NOTHING"),
                    connection);

            commandSelectFromNode.Parameters.AddWithValue("fromLatitude", source.Coordinate.Latitude);
            commandSelectFromNode.Parameters.AddWithValue("fromLongitude", source.Coordinate.Longitude);
            commandSelectFromNode.Parameters.AddWithValue("factor", MinFactor);
            commandSelectFromNode.Parameters.AddWithValue("minWeight", minWeight);
            commandSelectFromNode.Parameters.AddWithValue("vehicleType", VehicleType);
            commandSelectFromNode.Parameters.AddWithValue("profile", Profile);
            commandSelectFromNode.Parameters.AddWithValue("guid", Guid);
            commandSelectFromNode.Parameters.AddWithValue("size", size);
            commandSelectFromNode.Parameters.Add("node", NpgsqlDbType.Bigint);
            await commandSelectFromNode.PrepareAsync();

            void LoadEdgesAndNodes(long node)
            {
                commandSelectFromPrefetch.Parameters["node"].Value = node;

                if ((long)commandSelectFromPrefetch.ExecuteScalar()! != 0)
                    return;

                commandSelectFromNode.Parameters["node"].Value = node;

                commandBegin.ExecuteNonQuery();

                commandSelectFromNode.ExecuteNonQuery();

                commandCommit.ExecuteNonQuery();
            }

            await using (var command =
                         new NpgsqlCommand(@"INSERT INTO temp_dijkstra (
	                node,
	                weight,
                    g,
	                edge,
	                in_queue
                )
                VALUES (
	                @node,
                    @factor*distanceInMeters(@latitude,@longitude,@latitude1,@longitude1),
	                0,
	                null,
	                true
                )
                ON CONFLICT (node) DO NOTHING", connection))
            {
                command.Parameters.Add("node", NpgsqlDbType.Bigint);
                command.Parameters.Add("latitude1", NpgsqlDbType.Real);
                command.Parameters.Add("longitude1", NpgsqlDbType.Real);
                command.Parameters.AddWithValue("latitude", source.Coordinate.Latitude);
                command.Parameters.AddWithValue("longitude", source.Coordinate.Longitude);
                command.Parameters.AddWithValue("factor", MinFactor);
                await command.PrepareAsync();

                if (new[] { 0, 1, 3, 4 }.Contains(target.Direction))
                {
                    LoadEdgesAndNodes(target.FromNode);
                    command.Parameters["node"].Value = target.FromNode;
                    var coords = target.Coordinates.First();
                    command.Parameters["latitude1"].Value = coords.Latitude;
                    command.Parameters["longitude1"].Value = coords.Longitude;
                    await command.ExecuteNonQueryAsync();
                }

                if (new[] { 0, 2, 3, 5 }.Contains(target.Direction))
                {
                    LoadEdgesAndNodes(target.ToNode);
                    command.Parameters["node"].Value = target.ToNode;
                    var coords = target.Coordinates.Last();
                    command.Parameters["latitude1"].Value = coords.Latitude;
                    command.Parameters["longitude1"].Value = coords.Longitude;
                    await command.ExecuteNonQueryAsync();
                }
            }

            var steps = 0L;
            var node = 0L;
            float? weight = null;

            var sources = new List<long>();
            if (new[] { 0, 1, 3, 4 }.Contains(target.Direction)) sources.Add(source.ToNode);
            if (new[] { 0, 2, 3, 5 }.Contains(target.Direction)) sources.Add(source.FromNode);


            await using (var command =
                         new NpgsqlCommand(
                             string.Join(";",
                                 @"SELECT node,weight,NOT in_queue FROM temp_dijkstra WHERE node=ANY(@sources) ORDER BY weight LIMIT 1",
                                 @"SELECT node FROM temp_dijkstra WHERE in_queue ORDER BY weight LIMIT 1"),
                             connection))
            await using (var command2 =
                         new NpgsqlCommand(string.Join(";", @"INSERT INTO temp_dijkstra (
	                    node,
	                    weight,
	                    g,
	                    edge,
	                    in_queue
                    )
                    WITH cte AS
                    (
	                    SELECT *,ROW_NUMBER() OVER (PARTITION BY node ORDER BY weight) AS rn FROM (
		                    SELECT e.from_node AS node,n.from_weight+t.g+e.weight AS weight,t.g+e.weight AS g,e.id AS edge,true AS in_queue
		                    FROM temp_edge e JOIN temp_node n ON e.from_node=n.id JOIN temp_dijkstra t ON e.to_node=t.node
                            WHERE e.direction=ANY(ARRAY[0,1,3,4]) AND t.node=@node 
                            AND NOT EXISTS (SELECT * FROM temp_restriction WHERE via_node=t.node AND to_edge=t.edge AND from_edge=e.id)
                            UNION ALL SELECT e.to_node AS node,n.from_weight+t.g+e.weight AS weight,t.g+e.weight AS g,e.id AS edge,true AS in_queue
		                    FROM temp_edge e JOIN temp_node n ON e.to_node=n.id JOIN temp_dijkstra t ON e.from_node=t.node
                            WHERE e.direction=ANY(ARRAY[0,2,3,5]) AND t.node=@node
                            AND NOT EXISTS (SELECT * FROM temp_restriction WHERE via_node=t.node AND to_edge=t.edge AND from_edge=e.id)) q
                    )
                    SELECT 
	                    node,
	                    weight,
	                    g,
	                    edge,
	                    in_queue
                    FROM cte
                    WHERE rn = 1 AND weight<@maxWeight
                    ON CONFLICT (node) DO UPDATE SET
	                    weight=EXCLUDED.weight,
	                    g=EXCLUDED.g,
	                    edge=EXCLUDED.edge,
                        in_queue=EXCLUDED.in_queue
                        WHERE temp_dijkstra.weight>EXCLUDED.weight",
                             @"UPDATE temp_dijkstra SET in_queue=false WHERE node=@node",
                             @"DELETE FROM temp_dijkstra WHERE weight>@maxWeight"), connection))
            {
                command.Parameters.AddWithValue("sources", sources.ToArray());
                command2.Parameters.Add("maxWeight", NpgsqlDbType.Real);
                command2.Parameters.Add("node", NpgsqlDbType.Bigint);
                await command.PrepareAsync();
                await command2.PrepareAsync();

                for (;; steps++)
                {
                    var node1 = 0L;
                    await using (var reader = command.ExecuteReader())
                    {
                        if (reader.Read())
                        {
                            var maxWeightNew = reader.GetFloat(1);
                            if (maxWeightNew <= maxWeight)
                            {
                                weight = maxWeight = maxWeightNew;
                                node = reader.GetInt64(0);
                            }

                            if (reader.GetBoolean(2)) break;
                        }

                        reader.NextResult();

                        if (!reader.Read()) break;
                        node1 = reader.GetInt64(0);
                    }

                    LoadEdgesAndNodes(node1);

                    command2.Parameters["node"].Value = node1;
                    command2.Parameters["maxWeight"].Value = maxWeight;

                    command2.ExecuteNonQuery();
                }
            }


            await using (var command =
                         new NpgsqlCommand(@"SELECT e.from_node,e.id 
                    FROM temp_dijkstra t JOIN temp_edge e ON t.edge=e.id
                    WHERE t.node=@node AND e.to_node=t.node
                    UNION ALL SELECT e.to_node,e.id 
                    FROM temp_dijkstra t JOIN temp_edge e ON t.edge=e.id
                    WHERE t.node=@node AND e.from_node=t.node", connection))
            {
                command.Parameters.Add("node", NpgsqlDbType.Bigint);
                await command.PrepareAsync();
                var list = new List<long>();
                for (;;)
                {
                    command.Parameters["node"].Value = node;
                    await using var reader = await command.ExecuteReaderAsync();

                    if (!reader.Read()) break;

                    node = reader.GetInt64(0);
                    var edge = reader.GetInt64(1);
                    list.Add(edge);
                }

                stopWatch.Stop();

                Console.WriteLine($"{nameof(AStar)} steps={steps} ElapsedMilliseconds={stopWatch.ElapsedMilliseconds}");

                return new PathFinderResult
                {
                    Edges = list,
                    Weight = weight
                };
            }
        }
    }
}