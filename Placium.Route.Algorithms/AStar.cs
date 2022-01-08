using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using Placium.Route.Common;

namespace Placium.Route.Algorithms
{
    public class AStar : BasePathFinderAlgorithm
    {
        public AStar(Guid guid, string connectionString, string vehicleType, string profile, float minFactor, float maxFactor) : base(guid,
            connectionString, vehicleType, profile, minFactor,  maxFactor)
        {
        }


        public override async Task<PathFinderResult> FindPathAsync(RouterPoint source,
            RouterPoint target, float maxWeight = float.MaxValue)
        {
            var minWeight = MinFactor * 1;

            using var connection = new NpgsqlConnection(ConnectionString);
            using var connection2 = new NpgsqlConnection(ConnectionString);
            await connection.OpenAsync();
            await connection2.OpenAsync();

            using (var command =
                new NpgsqlCommand(string.Join(";", @"CREATE EXTENSION IF NOT EXISTS postgis WITH SCHEMA public",
                    @"create or replace function distanceInMeters(lat1 real, lon1 real, lat2 real, lon2 real)
                returns real
                language plpgsql
                as
                $$
                declare
                   dist real;
                begin
                   select ST_DistanceSphere(ST_MakePoint(lon1,lat1),ST_MakePoint(lon2,lat2)) 
                   into dist;
                   return dist;
                end
                $$", @"CREATE TEMP TABLE temp_node (
	                id BIGINT PRIMARY KEY NOT NULL, 
	                from_weight REAL NOT NULL
                )", @"CREATE TEMP TABLE temp_dijkstra (
	                node BIGINT PRIMARY KEY NOT NULL, 
	                weight REAL NOT NULL, 
	                weight1 REAL NOT NULL, 
	                edge BIGINT NOT NULL,
	                in_queue BOOLEAN NOT NULL
                )", @"CREATE TEMP TABLE shared_edge (
	                id BIGINT PRIMARY KEY NOT NULL, 
	                from_node BIGINT NOT NULL, 
	                to_node BIGINT NOT NULL, 
	                weight REAL NOT NULL,
                    direction SMALLINT NOT NULL
                )", @"CREATE TEMP TABLE shared_restriction (
	                id BIGINT PRIMARY KEY NOT NULL
                )", @"CREATE TEMP TABLE shared_restriction_from_edge (
	                rid BIGINT NOT NULL REFERENCES shared_restriction (id), 
	                edge BIGINT NOT NULL
                )", @"CREATE TEMP TABLE shared_restriction_to_edge (
	                rid BIGINT NOT NULL REFERENCES shared_restriction (id), 
	                edge BIGINT NOT NULL
                )", @"CREATE TEMP TABLE shared_restriction_via_node (
	                rid BIGINT NOT NULL REFERENCES shared_restriction (id), 
	                node BIGINT NOT NULL
                )"), connection))
            {
                command.Prepare();
                await command.ExecuteNonQueryAsync();
            }


            using var commandBegin =
                new NpgsqlCommand(@"BEGIN", connection);
            using var commandCommit =
                new NpgsqlCommand(@"COMMIT", connection);

            commandBegin.Prepare();
            commandCommit.Prepare();

            using (var command7 =
                new NpgsqlCommand(@"SELECT id FROM restriction WHERE vehicle_type=@vehicleType AND guid=@guid",
                    connection2))
            using (var command8 =
                new NpgsqlCommand(
                    @"SELECT rid,edge FROM restriction_from_edge WHERE vehicle_type=@vehicleType AND guid=@guid",
                    connection2))
            using (var command9 =
                new NpgsqlCommand(
                    @"SELECT rid,edge FROM restriction_to_edge WHERE vehicle_type=@vehicleType AND guid=@guid",
                    connection2))
            using (var command10 =
                new NpgsqlCommand(
                    @"SELECT rid,node FROM restriction_via_node WHERE vehicle_type=@vehicleType AND guid=@guid",
                    connection2))
            {
                command7.Parameters.AddWithValue("vehicleType", VehicleType);
                command7.Parameters.AddWithValue("guid", Guid);
                command7.Prepare();

                command8.Parameters.AddWithValue("vehicleType", VehicleType);
                command8.Parameters.AddWithValue("guid", Guid);
                command8.Prepare();

                command9.Parameters.AddWithValue("vehicleType", VehicleType);
                command9.Parameters.AddWithValue("guid", Guid);
                command9.Prepare();

                command10.Parameters.AddWithValue("vehicleType", VehicleType);
                command10.Parameters.AddWithValue("guid", Guid);
                command10.Prepare();

                await commandBegin.ExecuteNonQueryAsync();

                using (var writer = connection.BeginTextImport(
                    "COPY shared_restriction (id) FROM STDIN WITH NULL AS ''"))
                using (var reader = await command7.ExecuteReaderAsync())
                {
                    while (reader.Read())
                    {
                        var values = new[]
                        {
                            reader.GetInt64(0).ToString()
                        };
                        writer.WriteLine(string.Join("\t", values));
                    }
                }

                using (var writer = connection.BeginTextImport(
                    "COPY shared_restriction_from_edge (rid,edge) FROM STDIN WITH NULL AS ''"))
                using (var reader = await command8.ExecuteReaderAsync())
                {
                    while (reader.Read())
                    {
                        var values = new[]
                        {
                            reader.GetInt64(0).ToString(),
                            reader.GetInt64(1).ToString()
                        };
                        writer.WriteLine(string.Join("\t", values));
                    }
                }

                using (var writer = connection.BeginTextImport(
                    "COPY shared_restriction_to_edge (rid,edge) FROM STDIN WITH NULL AS ''"))
                using (var reader = await command9.ExecuteReaderAsync())
                {
                    while (reader.Read())
                    {
                        var values = new[]
                        {
                            reader.GetInt64(0).ToString(),
                            reader.GetInt64(1).ToString()
                        };
                        writer.WriteLine(string.Join("\t", values));
                    }
                }

                using (var writer = connection.BeginTextImport(
                    "COPY shared_restriction_via_node (rid,node) FROM STDIN WITH NULL AS ''"))
                using (var reader = await command10.ExecuteReaderAsync())
                {
                    while (reader.Read())
                    {
                        var values = new[]
                        {
                            reader.GetInt64(0).ToString(),
                            reader.GetInt64(1).ToString()
                        };
                        writer.WriteLine(string.Join("\t", values));
                    }
                }

                await commandCommit.ExecuteNonQueryAsync();
            }

            using (var command =
                new NpgsqlCommand(string.Join(";",
                        @"CREATE INDEX temp_dijkstra_in_queue_idx ON temp_dijkstra (in_queue)",
                        @"CREATE INDEX temp_dijkstra_weight_idx ON temp_dijkstra (weight)",
                        @"CREATE UNIQUE INDEX shared_edge_from_node_to_node_idx ON shared_edge (from_node,to_node)",
                        @"CREATE INDEX shared_restriction_from_edge_idx ON shared_restriction_from_edge (edge)",
                        @"CREATE INDEX shared_restriction_to_edge_idx ON shared_restriction_to_edge (edge)",
                        @"CREATE INDEX shared_restriction_via_node_idx ON shared_restriction_via_node (node)"),
                    connection))
            {
                command.Prepare();
                await command.ExecuteNonQueryAsync();
            }

            using var commandInsertIntoNode = new NpgsqlCommand(
                @"INSERT INTO temp_node (id,from_weight) VALUES (@id,
                @fromWeight)
                ON CONFLICT (id) DO NOTHING",
                connection);
            using var commandInsertIntoEdge = new NpgsqlCommand(
                @"INSERT INTO shared_edge (id,from_node,to_node,
                weight,direction) VALUES (@id,@fromNode,@toNode,
                    @weight,@direction)
                ON CONFLICT (from_node,to_node) DO NOTHING",
                connection);
            using var commandSelectFromNode =
                new NpgsqlCommand(
                    @"SELECT n.id,
                    @factor*distanceInMeters(latitude,longitude,@fromLatitude,@fromLongitude)
                    FROM node n JOIN edge e ON n.id=e.to_node WHERE n.guid=@guid AND e.guid=@guid
                    AND @factor*distanceInMeters(latitude,longitude,@fromLatitude,@fromLongitude)<=@maxWeight
                    AND @node=ANY(e.nodes) UNION ALL SELECT n.id,
                    @factor*distanceInMeters(latitude,longitude,@fromLatitude,@fromLongitude)
                    FROM node n JOIN edge e ON n.id=e.from_node WHERE n.guid=@guid AND e.guid=@guid
                    AND @factor*distanceInMeters(latitude,longitude,@fromLatitude,@fromLongitude)<=@maxWeight
                    AND @node=ANY(e.nodes)",
                    connection2);
            using var commandSelectFromEdge =
                new NpgsqlCommand(
                    @"SELECT id,from_node,to_node,
                    GREATEST((weight->@profile)::real,@minWeight),(direction->@profile)::smallint
                    FROM edge WHERE weight?@profile AND direction?@profile AND guid=@guid
                    AND @node=ANY(nodes)",
                    connection2);

            commandInsertIntoNode.Parameters.Add("id", NpgsqlDbType.Bigint);
            commandInsertIntoNode.Parameters.Add("fromWeight", NpgsqlDbType.Real);
            commandInsertIntoNode.Prepare();

            commandInsertIntoEdge.Parameters.Add("id", NpgsqlDbType.Bigint);
            commandInsertIntoEdge.Parameters.Add("fromNode", NpgsqlDbType.Bigint);
            commandInsertIntoEdge.Parameters.Add("toNode", NpgsqlDbType.Bigint);
            commandInsertIntoEdge.Parameters.Add("weight", NpgsqlDbType.Real);
            commandInsertIntoEdge.Parameters.Add("direction", NpgsqlDbType.Smallint);
            commandInsertIntoEdge.Prepare();

            commandSelectFromNode.Parameters.AddWithValue("fromLatitude", source.Coordinate.Latitude);
            commandSelectFromNode.Parameters.AddWithValue("fromLongitude", source.Coordinate.Longitude);
            commandSelectFromNode.Parameters.AddWithValue("maxWeight", maxWeight);
            commandSelectFromNode.Parameters.AddWithValue("factor", MinFactor);
            commandSelectFromNode.Parameters.AddWithValue("guid", Guid);
            commandSelectFromNode.Parameters.Add("node", NpgsqlDbType.Bigint);
            commandSelectFromNode.Prepare();

            commandSelectFromEdge.Parameters.AddWithValue("minWeight", minWeight);
            commandSelectFromEdge.Parameters.AddWithValue("profile", Profile);
            commandSelectFromEdge.Parameters.AddWithValue("guid", Guid);
            commandSelectFromEdge.Parameters.Add("node", NpgsqlDbType.Bigint);
            commandSelectFromEdge.Prepare();

            void LoadEdgesAndNodes(long node)
            {
                commandSelectFromNode.Parameters["node"].Value = node;
                commandSelectFromEdge.Parameters["node"].Value = node;

                commandBegin.ExecuteNonQuery();

                using (var reader = commandSelectFromNode.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        commandInsertIntoNode.Parameters["id"].Value = reader.GetInt64(0);
                        commandInsertIntoNode.Parameters["fromWeight"].Value = reader.GetFloat(1);

                        commandInsertIntoNode.ExecuteNonQuery();
                    }
                }

                using (var reader = commandSelectFromEdge.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        commandInsertIntoEdge.Parameters["id"].Value = reader.GetInt64(0);
                        commandInsertIntoEdge.Parameters["fromNode"].Value = reader.GetInt64(1);
                        commandInsertIntoEdge.Parameters["toNode"].Value = reader.GetInt64(2);
                        commandInsertIntoEdge.Parameters["weight"].Value = reader.GetFloat(3);
                        commandInsertIntoEdge.Parameters["direction"].Value = reader.GetInt16(4);

                        commandInsertIntoEdge.ExecuteNonQuery();
                    }
                }

                commandCommit.ExecuteNonQuery();
            }

            using (var command =
                new NpgsqlCommand(@"INSERT INTO temp_dijkstra (
	                node,
	                weight,
                    weight1,
	                edge,
	                in_queue
                )
                VALUES (
	                @node,
                    @factor*distanceInMeters(@latitude,@longitude,@latitude1,@longitude1),
	                0,
	                0,
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
                command.Prepare();

                if (new[] {0, 1, 3, 4}.Contains(target.Direction))
                {
                    command.Parameters["node"].Value = target.FromNode;
                    var coords = target.Coordinates.First();
                    command.Parameters["latitude1"].Value = coords.Latitude;
                    command.Parameters["longitude1"].Value = coords.Longitude;
                    await command.ExecuteNonQueryAsync();
                }

                if (new[] {0, 2, 3, 5}.Contains(target.Direction))
                {
                    command.Parameters["node"].Value = target.ToNode;
                    var coords = target.Coordinates.Last();
                    command.Parameters["latitude1"].Value = coords.Latitude;
                    command.Parameters["longitude1"].Value = coords.Longitude;
                    await command.ExecuteNonQueryAsync();
                }
            }

            var node = 0L;
            float? weight = null;

            var sources = new List<long>();
            if (new[] {0, 1, 3, 4}.Contains(target.Direction)) sources.Add(source.ToNode);
            if (new[] {0, 2, 3, 5}.Contains(target.Direction)) sources.Add(source.FromNode);


            using (var command =
                new NpgsqlCommand(
                    @"SELECT node,weight,NOT in_queue FROM temp_dijkstra WHERE node=ANY(@sources) ORDER BY weight LIMIT 1",
                    connection))
            using (var command1 =
                new NpgsqlCommand(
                    string.Join(";", @"SELECT node FROM temp_dijkstra WHERE in_queue ORDER BY weight LIMIT 1"),
                    connection))
            using (var command2 =
                new NpgsqlCommand(string.Join(";", @"INSERT INTO temp_dijkstra (
	                    node,
	                    weight,
	                    weight1,
	                    edge,
	                    in_queue
                    )
                    WITH cte AS
                    (
	                    SELECT *,ROW_NUMBER() OVER (PARTITION BY node ORDER BY weight) AS rn FROM (
		                    SELECT e.from_node AS node,n.from_weight+t.weight1+e.weight AS weight,t.weight1+e.weight AS weight1,e.id AS edge,true AS in_queue
		                    FROM shared_edge e JOIN temp_node n ON e.from_node=n.id JOIN temp_dijkstra t ON e.to_node=t.node
                            WHERE e.direction=ANY(ARRAY[0,1,3,4]) AND t.node=@node 
                            AND NOT EXISTS (SELECT * FROM shared_restriction r 
                            JOIN shared_restriction_via_node vn ON vn.node=t.node AND r.id=vn.rid
                            JOIN shared_restriction_to_edge rt ON rt.edge=e.id AND r.id=rt.rid
                            JOIN shared_restriction_from_edge rf ON rf.edge=t.edge AND r.id=rf.rid)
                            UNION ALL SELECT e.to_node AS node,n.from_weight+t.weight1+e.weight AS weight,t.weight1+e.weight AS weight1,e.id AS edge,true AS in_queue
		                    FROM shared_edge e JOIN temp_node n ON e.to_node=n.id JOIN temp_dijkstra t ON e.from_node=t.node
                            WHERE e.direction=ANY(ARRAY[0,2,3,5]) AND t.node=@node
                            AND NOT EXISTS (SELECT * FROM shared_restriction r 
                            JOIN shared_restriction_via_node vn ON vn.node=t.node AND r.id=vn.rid
                            JOIN shared_restriction_to_edge rt ON rt.edge=e.id AND r.id=rt.rid
                            JOIN shared_restriction_from_edge rf ON rf.edge=t.edge AND r.id=rf.rid)) q
                    )
                    SELECT 
	                    node,
	                    weight,
	                    weight1,
	                    edge,
	                    in_queue
                    FROM cte
                    WHERE rn = 1 AND weight<@maxWeight
                    ON CONFLICT (node) DO UPDATE SET
	                    weight=EXCLUDED.weight,
	                    weight1=EXCLUDED.weight1,
	                    edge=EXCLUDED.edge,
                        in_queue=EXCLUDED.in_queue
                        WHERE temp_dijkstra.weight>EXCLUDED.weight",
                    @"UPDATE temp_dijkstra SET in_queue=false WHERE node=@node",
                    @"DELETE FROM temp_dijkstra WHERE weight>@maxWeight"), connection))
            {
                command.Parameters.AddWithValue("sources", sources.ToArray());
                command2.Parameters.Add("maxWeight", NpgsqlDbType.Real);
                command2.Parameters.Add("node", NpgsqlDbType.Bigint);
                command.Prepare();
                command1.Prepare();
                command2.Prepare();

                for (var step = 0L;; step++)
                {
                    using (var reader = command.ExecuteReader())
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
                    }

                    var node1 = 0L;
                    using (var reader = command1.ExecuteReader())
                    {
                        if (!reader.Read()) break;
                        node1 = reader.GetInt64(0);
                    }

                    LoadEdgesAndNodes(node1);

                    command2.Parameters["node"].Value = node1;
                    command2.Parameters["maxWeight"].Value = maxWeight;

                    command2.ExecuteNonQuery();


                    if (step % 100 == 0) Console.WriteLine($"{DateTime.Now:O} Step {step} complete");
                }
            }


            using (var command =
                new NpgsqlCommand(@"SELECT e.from_node,e.id 
                    FROM temp_dijkstra t JOIN shared_edge e ON t.edge=e.id
                    WHERE t.node=@node AND e.to_node=t.node
                    UNION ALL SELECT e.to_node,e.id 
                    FROM temp_dijkstra t JOIN shared_edge e ON t.edge=e.id
                    WHERE t.node=@node AND e.from_node=t.node", connection))
            {
                command.Parameters.Add("node", NpgsqlDbType.Bigint);
                command.Prepare();
                var list = new List<long>();
                for (;;)
                {
                    command.Parameters["node"].Value = node;
                    using var reader = await command.ExecuteReaderAsync();

                    if (!reader.Read()) break;

                    node = reader.GetInt64(0);
                    var edge = reader.GetInt64(1);
                    list.Add(edge);
                }

                return new PathFinderResult
                {
                    Edges = list,
                    Weight = weight
                };
            }
        }
    }
}