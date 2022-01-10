using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using Npgsql;
using NpgsqlTypes;
using Placium.Route.Common;

namespace Placium.Route.Algorithms
{
    public class MemoryBidirectionalDijkstra : BasePathFinderAlgorithm
    {
        public MemoryBidirectionalDijkstra(Guid guid, string connectionString, string vehicleType, string profile,
            float minFactor, float maxFactor) :
            base(guid,
                connectionString, vehicleType, profile, minFactor, maxFactor)
        {
        }


        public override async Task<PathFinderResult> FindPathAsync(RouterPoint source,
            RouterPoint target, float maxWeight = float.MaxValue)
        {
            var minWeight = MinFactor * 1;

            using var connection = new SqliteConnection("Data source=:memory:");
            using var connection2 = new NpgsqlConnection(ConnectionString);
            await connection.OpenAsync();
            await connection2.OpenAsync();


            using (var command =
                new SqliteCommand(string.Join(";", "PRAGMA synchronous = OFF",
                    @"CREATE TEMP TABLE temp_node (
	                id INTEGER PRIMARY KEY NOT NULL
                )", @"CREATE TEMP TABLE temp_dijkstra1 (
	                node INTEGER PRIMARY KEY NOT NULL, 
	                weight REAL NOT NULL, 
	                edge INTEGER NOT NULL,
	                in_queue INTEGER NOT NULL
                )", @"CREATE TEMP TABLE temp_dijkstra2 (
	                node INTEGER PRIMARY KEY NOT NULL, 
	                weight REAL NOT NULL, 
	                edge INTEGER NOT NULL,
	                in_queue INTEGER NOT NULL
                )", @"CREATE TEMP TABLE temp_edge (
	                id INTEGER PRIMARY KEY NOT NULL, 
	                from_node INTEGER NOT NULL, 
	                to_node INTEGER NOT NULL,
	                weight REAL NOT NULL,
                    direction INTEGER NOT NULL
                )", @"CREATE TEMP TABLE temp_restriction (
	                id INTEGER PRIMARY KEY NOT NULL
                )", @"CREATE TEMP TABLE temp_restriction_from_edge (
	                rid INTEGER NOT NULL, 
	                edge INTEGER NOT NULL,
                    FOREIGN KEY(rid) REFERENCES temp_restriction(id)
                )", @"CREATE TEMP TABLE temp_restriction_to_edge (
	                rid INTEGER NOT NULL, 
	                edge INTEGER NOT NULL,
                    FOREIGN KEY(rid) REFERENCES temp_restriction(id)
                )", @"CREATE TEMP TABLE temp_restriction_via_node (
	                rid INTEGER NOT NULL, 
	                node INTEGER NOT NULL,
                    FOREIGN KEY(rid) REFERENCES temp_restriction(id)
                )"), connection))
            {
                command.Prepare();
                await command.ExecuteNonQueryAsync();
            }


            using (var command =
                new SqliteCommand(string.Join(";",
                        @"CREATE INDEX temp_dijkstra1_in_queue_idx ON temp_dijkstra1 (in_queue)",
                        @"CREATE INDEX temp_dijkstra1_weight_idx ON temp_dijkstra1 (weight)",
                        @"CREATE INDEX temp_dijkstra2_in_queue_idx ON temp_dijkstra2 (in_queue)",
                        @"CREATE INDEX temp_dijkstra2_weight_idx ON temp_dijkstra2 (weight)",
                        @"CREATE UNIQUE INDEX temp_edge_from_node_to_node_idx ON temp_edge (from_node,to_node)",
                        @"CREATE INDEX temp_restriction_from_edge_idx ON temp_restriction_from_edge (edge)",
                        @"CREATE INDEX temp_restriction_to_edge_idx ON temp_restriction_to_edge (edge)",
                        @"CREATE INDEX temp_restriction_via_node_idx ON temp_restriction_via_node (node)"),
                    connection))
            {
                command.Prepare();
                await command.ExecuteNonQueryAsync();
            }

            using var commandBegin =
                new SqliteCommand(@"BEGIN TRANSACTION",
                    connection);
            using var commandCommit =
                new SqliteCommand(@"COMMIT",
                    connection);

            commandBegin.Prepare();
            commandCommit.Prepare();


            using var commandInsertIntoRestriction =
                new SqliteCommand(@"INSERT INTO temp_restriction(id) VALUES (@id) ON CONFLICT DO NOTHING",
                    connection);
            using var commandInsertIntoRestrictionFromEdge =
                new SqliteCommand(
                    @"INSERT INTO temp_restriction_from_edge(rid,edge) VALUES (@id,@edge) ON CONFLICT DO NOTHING",
                    connection);
            using var commandInsertIntoRestrictionToEdge =
                new SqliteCommand(
                    @"INSERT INTO temp_restriction_to_edge(rid,edge) VALUES (@id,@edge) ON CONFLICT DO NOTHING",
                    connection);
            using var commandInsertIntoRestrictionViaNode =
                new SqliteCommand(
                    @"INSERT INTO temp_restriction_via_node(rid,node) VALUES (@id,@node) ON CONFLICT DO NOTHING",
                    connection);

            commandInsertIntoRestriction.Parameters.Add("id", SqliteType.Integer);
            commandInsertIntoRestriction.Prepare();

            commandInsertIntoRestrictionFromEdge.Parameters.Add("id", SqliteType.Integer);
            commandInsertIntoRestrictionFromEdge.Parameters.Add("edge", SqliteType.Integer);
            commandInsertIntoRestrictionFromEdge.Prepare();

            commandInsertIntoRestrictionToEdge.Parameters.Add("id", SqliteType.Integer);
            commandInsertIntoRestrictionToEdge.Parameters.Add("edge", SqliteType.Integer);
            commandInsertIntoRestrictionToEdge.Prepare();

            commandInsertIntoRestrictionViaNode.Parameters.Add("id", SqliteType.Integer);
            commandInsertIntoRestrictionViaNode.Parameters.Add("node", SqliteType.Integer);
            commandInsertIntoRestrictionViaNode.Prepare();

            using var commandSelectFromRestriction =
                new NpgsqlCommand(string.Join(";", @"SELECT rid FROM (
                    SELECT rid FROM restriction_via_node WHERE node=@node AND vehicle_type=@vehicleType AND guid=@guid
                    UNION ALL SELECT rid FROM restriction_from_edge r JOIN edge e ON r.edge=e.id
                    WHERE (e.from_node=@node OR e.to_node=@node) AND r.vehicle_type=@vehicleType AND r.guid=@guid AND e.guid=@guid
                    UNION ALL SELECT rid FROM restriction_to_edge r JOIN edge e ON r.edge=e.id
                    WHERE (e.from_node=@node OR e.to_node=@node) AND r.vehicle_type=@vehicleType AND r.guid=@guid AND e.guid=@guid) q
                    GROUP BY rid",
                        @"SELECT r.rid,r.edge FROM restriction_from_edge r JOIN edge e ON r.edge=e.id
                    WHERE (e.from_node=@node OR e.to_node=@node) AND r.vehicle_type=@vehicleType AND r.guid=@guid AND e.guid=@guid
                    GROUP BY r.rid,r.edge",
                        @"SELECT r.rid,r.edge FROM restriction_to_edge r JOIN edge e ON r.edge=e.id
                    WHERE (e.from_node=@node OR e.to_node=@node) AND r.vehicle_type=@vehicleType AND r.guid=@guid AND e.guid=@guid
                    GROUP BY r.rid,r.edge",
                        @"SELECT rid,node FROM restriction_via_node WHERE node=@node AND vehicle_type=@vehicleType AND guid=@guid"),
                    connection2);


            commandSelectFromRestriction.Parameters.Add("node", NpgsqlDbType.Bigint);
            commandSelectFromRestriction.Parameters.AddWithValue("vehicleType", VehicleType);
            commandSelectFromRestriction.Parameters.AddWithValue("guid", Guid);
            commandSelectFromRestriction.Prepare();

            using var commandInsertIntoNode = new SqliteCommand(
                @"INSERT INTO temp_node (id) VALUES (@id)
                ON CONFLICT (id) DO NOTHING",
                connection);
            using var commandInsertIntoEdge = new SqliteCommand(
                @"INSERT INTO temp_edge (id,from_node,to_node,weight,direction)
                VALUES (@id,@fromNode,@toNode,@weight,@direction)
                ON CONFLICT (from_node,to_node) DO NOTHING",
                connection);
            using var commandSelectFromNode =
                new NpgsqlCommand(string.Join(";",
                        @"SELECT n.id
                    FROM node n JOIN edge e ON n.id=e.from_node WHERE n.guid=@guid AND e.guid=@guid
                    AND e.to_node=@node UNION ALL SELECT n.id
                    FROM node n JOIN edge e ON n.id=e.to_node WHERE n.guid=@guid AND e.guid=@guid
                    AND e.from_node=@node", @"SELECT id,from_node,to_node,
                    GREATEST((weight->@profile)::real,@minWeight),(direction->@profile)::smallint
                    FROM edge WHERE weight?@profile AND direction?@profile AND guid=@guid
                    AND (from_node=@node OR to_node=@node)"),
                    connection2);

            commandInsertIntoNode.Parameters.Add("id", SqliteType.Integer);
            commandInsertIntoNode.Prepare();

            commandInsertIntoEdge.Parameters.Add("id", SqliteType.Integer);
            commandInsertIntoEdge.Parameters.Add("fromNode", SqliteType.Integer);
            commandInsertIntoEdge.Parameters.Add("toNode", SqliteType.Integer);
            commandInsertIntoEdge.Parameters.Add("weight", SqliteType.Real);
            commandInsertIntoEdge.Parameters.Add("direction", SqliteType.Integer);
            commandInsertIntoEdge.Prepare();

            commandSelectFromNode.Parameters.AddWithValue("minWeight", minWeight);
            commandSelectFromNode.Parameters.AddWithValue("profile", Profile);
            commandSelectFromNode.Parameters.AddWithValue("guid", Guid);
            commandSelectFromNode.Parameters.Add("node", NpgsqlDbType.Bigint);
            commandSelectFromNode.Prepare();

            void LoadEdgesAndNodes(long node)
            {
                commandSelectFromNode.Parameters["node"].Value = node;
                commandSelectFromRestriction.Parameters["node"].Value = node;

                commandBegin.ExecuteNonQuery();

                using (var reader = commandSelectFromNode.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        commandInsertIntoNode.Parameters["id"].Value = reader.GetInt64(0);

                        commandInsertIntoNode.ExecuteNonQuery();
                    }

                    reader.NextResult();

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

                using (var reader = commandSelectFromRestriction.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        commandInsertIntoRestriction.Parameters["id"].Value = reader.GetInt64(0);
                        commandInsertIntoRestriction.ExecuteNonQuery();
                    }

                    reader.NextResult();

                    while (reader.Read())
                    {
                        commandInsertIntoRestrictionFromEdge.Parameters["id"].Value = reader.GetInt64(0);
                        commandInsertIntoRestrictionFromEdge.Parameters["edge"].Value = reader.GetInt64(1);
                        commandInsertIntoRestrictionFromEdge.ExecuteNonQuery();
                    }

                    reader.NextResult();

                    while (reader.Read())
                    {
                        commandInsertIntoRestrictionToEdge.Parameters["id"].Value = reader.GetInt64(0);
                        commandInsertIntoRestrictionToEdge.Parameters["edge"].Value = reader.GetInt64(1);
                        commandInsertIntoRestrictionToEdge.ExecuteNonQuery();
                    }

                    reader.NextResult();

                    while (reader.Read())
                    {
                        commandInsertIntoRestrictionViaNode.Parameters["id"].Value = reader.GetInt64(0);
                        commandInsertIntoRestrictionViaNode.Parameters["node"].Value = reader.GetInt64(1);
                        commandInsertIntoRestrictionViaNode.ExecuteNonQuery();
                    }
                }


                commandCommit.ExecuteNonQuery();
            }

            using (var command =
                new SqliteCommand(@"REPLACE INTO temp_dijkstra1 (
	                node,
	                weight,
	                edge,
	                in_queue
                )
                VALUES (
	                @node,
	                0,
	                0,
	                1
                )", connection))
            {
                command.Parameters.Add("node", SqliteType.Integer);
                command.Prepare();

                if (new[] {0, 1, 3, 4}.Contains(source.Direction))
                {
                    command.Parameters["node"].Value = source.ToNode;
                    await command.ExecuteNonQueryAsync();
                }

                if (new[] {0, 2, 3, 5}.Contains(source.Direction))
                {
                    command.Parameters["node"].Value = source.FromNode;
                    await command.ExecuteNonQueryAsync();
                }
            }

            using (var command =
                new SqliteCommand(@"REPLACE INTO temp_dijkstra2 (
	                node,
	                weight,
	                edge,
	                in_queue
                )
                VALUES (
	                @node,
	                0,
	                0,
	                1
                )", connection))
            {
                command.Parameters.Add("node", SqliteType.Integer);
                command.Prepare();

                if (new[] {0, 1, 3, 4}.Contains(target.Direction))
                {
                    command.Parameters["node"].Value = target.FromNode;
                    await command.ExecuteNonQueryAsync();
                }

                if (new[] {0, 2, 3, 5}.Contains(target.Direction))
                {
                    command.Parameters["node"].Value = target.ToNode;
                    await command.ExecuteNonQueryAsync();
                }
            }

            var node = 0L;
            float? weight = null;

            using (var command =
                new SqliteCommand(string.Join(";",
                        @"SELECT t1.node,t1.weight+t2.weight,NOT t1.in_queue AND NOT t2.in_queue
                    FROM temp_dijkstra1 t1 JOIN temp_dijkstra2 t2 ON t1.node=t2.node ORDER BY t1.weight+t2.weight LIMIT 1",
                        @"SELECT node,weight FROM temp_dijkstra1 WHERE in_queue ORDER BY weight LIMIT 1",
                        @"SELECT node,weight FROM temp_dijkstra2 WHERE in_queue ORDER BY weight LIMIT 1"),
                    connection))
            using (var commandStep1 =
                new SqliteCommand(string.Join(";", @"INSERT INTO temp_dijkstra1 (
	                    node,
	                    weight,
	                    edge,
	                    in_queue
                    )
                    WITH cte AS
                    (
	                    SELECT *,ROW_NUMBER() OVER (PARTITION BY node ORDER BY weight) AS rn FROM (
		                    SELECT e.to_node AS node,t.weight+e.weight AS weight,e.id AS edge,1 AS in_queue
		                    FROM temp_edge e JOIN temp_dijkstra1 t ON e.from_node=t.node
                            WHERE (e.direction=0 OR e.direction=1 OR e.direction=3 OR e.direction=4) AND t.node=@node
                            AND NOT EXISTS (SELECT * FROM temp_restriction r 
                            JOIN temp_restriction_via_node vn ON vn.node=t.node AND r.id=vn.rid
                            JOIN temp_restriction_to_edge rt ON rt.edge=e.id AND r.id=rt.rid
                            JOIN temp_restriction_from_edge rf ON rf.edge=t.edge AND r.id=rf.rid)
                            UNION ALL SELECT e.from_node AS node,t.weight+e.weight AS weight,e.id AS edge,1 AS in_queue
		                    FROM temp_edge e JOIN temp_dijkstra1 t ON e.to_node=t.node
                            WHERE (e.direction=0 OR e.direction=2 OR e.direction=3 OR e.direction=5) AND t.node=@node
                            AND NOT EXISTS (SELECT * FROM temp_restriction r 
                            JOIN temp_restriction_via_node vn ON vn.node=t.node AND r.id=vn.rid
                            JOIN temp_restriction_to_edge rt ON rt.edge=e.id AND r.id=rt.rid
                            JOIN temp_restriction_from_edge rf ON rf.edge=t.edge AND r.id=rf.rid)) q
                    )
                    SELECT 
	                    node,
	                    weight,
	                    edge,
	                    in_queue
                    FROM cte
                    WHERE rn = 1 AND weight<@maxWeight
                    ON CONFLICT (node) DO UPDATE SET
	                    weight=EXCLUDED.weight,
	                    edge=EXCLUDED.edge,
                        in_queue=EXCLUDED.in_queue
                        WHERE temp_dijkstra1.weight>EXCLUDED.weight",
                    @"UPDATE temp_dijkstra1 SET in_queue=0 WHERE node=@node",
                    @"DELETE FROM temp_dijkstra1 WHERE weight>@maxWeight"), connection))
            using (var commandStep2 =
                new SqliteCommand(string.Join(";", @"INSERT INTO temp_dijkstra2 (
	                    node,
	                    weight,
	                    edge,
	                    in_queue
                    )
                    WITH cte AS
                    (
	                    SELECT *,ROW_NUMBER() OVER (PARTITION BY node ORDER BY weight) AS rn FROM (
		                    SELECT e.from_node AS node,t.weight+e.weight AS weight,e.id AS edge,1 AS in_queue
		                    FROM temp_edge e JOIN temp_dijkstra2 t ON e.to_node=t.node
                            WHERE (e.direction=0 OR e.direction=1 OR e.direction=3 OR e.direction=4) AND t.node=@node
                            AND NOT EXISTS (SELECT * FROM temp_restriction r 
                            JOIN temp_restriction_via_node vn ON vn.node=t.node AND r.id=vn.rid
                            JOIN temp_restriction_from_edge rf ON rf.edge=e.id AND r.id=rf.rid
                            JOIN temp_restriction_to_edge rt ON rt.edge=t.edge AND r.id=rt.rid)
                            UNION ALL SELECT e.to_node AS node,t.weight+e.weight AS weight,e.id AS edge,1 AS in_queue
		                    FROM temp_edge e JOIN temp_dijkstra2 t ON e.from_node=t.node
                            WHERE (e.direction=0 OR e.direction=2 OR e.direction=3 OR e.direction=5) AND t.node=@node
                            AND NOT EXISTS (SELECT * FROM temp_restriction r 
                            JOIN temp_restriction_via_node vn ON vn.node=t.node AND r.id=vn.rid
                            JOIN temp_restriction_from_edge rf ON rf.edge=e.id AND r.id=rf.rid
                            JOIN temp_restriction_to_edge rt ON rt.edge=t.edge AND r.id=rt.rid)) q
                    )
                    SELECT 
	                    node,
	                    weight,
	                    edge,
	                    in_queue
                    FROM cte
                    WHERE rn = 1 AND weight<@maxWeight
                    ON CONFLICT (node) DO UPDATE SET
	                    weight=EXCLUDED.weight,
	                    edge=EXCLUDED.edge,
                        in_queue=EXCLUDED.in_queue
                        WHERE temp_dijkstra2.weight>EXCLUDED.weight",
                    @"UPDATE temp_dijkstra2 SET in_queue=0 WHERE node=@node",
                    @"DELETE FROM temp_dijkstra2 WHERE weight>@maxWeight"), connection))
            {
                commandStep1.Parameters.Add("maxWeight", SqliteType.Real);
                commandStep1.Parameters.Add("node", SqliteType.Integer);
                commandStep2.Parameters.Add("maxWeight", SqliteType.Real);
                commandStep2.Parameters.Add("node", SqliteType.Integer);
                command.Prepare();
                commandStep1.Prepare();
                commandStep2.Prepare();

                for (var step = 0L;; step++)
                {
                    var node1 = 0L;
                    var node2 = 0L;
                    var weight1 = 0f;
                    var weight2 = 0f;

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
                        }

                        reader.NextResult();

                        if (!reader.Read()) break;
                        node1 = reader.GetInt64(0);
                        weight1 = reader.GetFloat(1);

                        reader.NextResult();

                        if (!reader.Read()) break;
                        node2 = reader.GetInt64(0);
                        weight2 = reader.GetFloat(1);
                    }

                    if (maxWeight <= weight1 + weight2 + minWeight) break;


                    if (weight1 < weight2)
                    {
                        LoadEdgesAndNodes(node1);

                        commandStep1.Parameters["node"].Value = node1;
                        commandStep1.Parameters["maxWeight"].Value = maxWeight;

                        commandStep1.ExecuteNonQuery();
                    }
                    else
                    {
                        LoadEdgesAndNodes(node2);

                        commandStep2.Parameters["node"].Value = node2;
                        commandStep2.Parameters["maxWeight"].Value = maxWeight;

                        commandStep2.ExecuteNonQuery();
                    }


                    if (step % 100 == 0)
                        Console.WriteLine($"{DateTime.Now:O} Step {step} complete" +
                                          $" temp_dijkstra1={new SqliteCommand("SELECT COUNT(*) FROM temp_dijkstra1 WHERE in_queue", connection).ExecuteScalar()}" +
                                          $" temp_dijkstra2={new SqliteCommand("SELECT COUNT(*) FROM temp_dijkstra2 WHERE in_queue", connection).ExecuteScalar()}" +
                                          $" MIN(weight1)={new SqliteCommand("SELECT MIN(weight) FROM temp_dijkstra1 WHERE in_queue", connection).ExecuteScalar()}" +
                                          $" MIN(weight2)={new SqliteCommand("SELECT MIN(weight) FROM temp_dijkstra2 WHERE in_queue", connection).ExecuteScalar()}");
                }
            }

            var list = new List<long>();

            using (var command =
                new SqliteCommand(@"SELECT e.from_node,e.id 
                    FROM temp_dijkstra1 t JOIN temp_edge e ON t.edge=e.id
                    WHERE t.node=@node AND e.to_node=t.node
                    UNION ALL SELECT e.to_node,e.id 
                    FROM temp_dijkstra1 t JOIN temp_edge e ON t.edge=e.id
                    WHERE t.node=@node AND e.from_node=t.node", connection))
            {
                command.Parameters.Add("node", SqliteType.Integer);
                command.Prepare();
                for (var node1 = node;;)
                {
                    command.Parameters["node"].Value = node1;
                    using var reader = await command.ExecuteReaderAsync();

                    if (!reader.Read()) break;

                    node1 = reader.GetInt64(0);
                    list.Add(reader.GetInt64(1));
                }
            }

            list.Reverse();

            using (var command =
                new SqliteCommand(@"SELECT e.from_node,e.id 
                    FROM temp_dijkstra2 t JOIN temp_edge e ON t.edge=e.id
                    WHERE t.node=@node AND e.to_node=t.node
                    UNION ALL SELECT e.to_node,e.id 
                    FROM temp_dijkstra2 t JOIN temp_edge e ON t.edge=e.id
                    WHERE t.node=@node AND e.from_node=t.node", connection))
            {
                command.Parameters.Add("node", SqliteType.Integer);
                command.Prepare();

                for (var node2 = node;;)
                {
                    command.Parameters["node"].Value = node2;
                    using var reader = await command.ExecuteReaderAsync();

                    if (!reader.Read()) break;

                    node2 = reader.GetInt64(0);
                    var edge = reader.GetInt64(1);
                    list.Add(edge);
                }
            }

            return new PathFinderResult
            {
                Edges = list,
                Weight = weight
            };
        }
    }
}