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
    public class MemoryAStar : BasePathFinderAlgorithm
    {
        public MemoryAStar(Guid guid, string connectionString, string vehicleType, string profile, float minFactor, float maxFactor) :
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


            connection.CreateFunction(
                "distanceInMeters",
                (double lat1, double lon1, double lat2, double lon2) =>
                {
                    const double R = 6371000; // metres
                    var φ1 = lat1 * Math.PI / 180; // φ, λ in radians
                    var φ2 = lat2 * Math.PI / 180;
                    var Δφ = (lat2 - lat1) * Math.PI / 180;
                    var Δλ = (lon2 - lon1) * Math.PI / 180;

                    var a = Math.Pow(Math.Sin(Δφ / 2), 2) +
                            Math.Cos(φ1) * Math.Cos(φ2) *
                            Math.Pow(Math.Sin(Δλ / 2), 2);
                    var c = 2 * Math.Atan2(Math.Sqrt(a), Math.Sqrt(1 - a));

                    return R * c; // in metres
                });

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

            using (var command =
                new SqliteCommand(string.Join(";", "PRAGMA synchronous = OFF",
                    @"CREATE TEMP TABLE temp_node (
	                id INTEGER PRIMARY KEY NOT NULL, 
	                from_weight REAL NOT NULL
                )", @"CREATE TEMP TABLE temp_dijkstra (
	                node INTEGER PRIMARY KEY NOT NULL, 
	                weight REAL NOT NULL, 
	                g REAL NOT NULL, 
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
                        @"CREATE INDEX temp_dijkstra_in_queue_idx ON temp_dijkstra (in_queue)",
                        @"CREATE INDEX temp_dijkstra_weight_idx ON temp_dijkstra (weight)",
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
                @"INSERT INTO temp_node (id,from_weight) VALUES (@id,@fromWeight)
                ON CONFLICT (id) DO NOTHING",
                connection);
            using var commandInsertIntoEdge = new SqliteCommand(
                @"INSERT INTO temp_edge (id,from_node,to_node,weight,direction)
                VALUES (@id,@fromNode,@toNode,@weight,@direction)
                ON CONFLICT (from_node,to_node) DO NOTHING",
                connection);
            using var commandSelectFromNode =
                new NpgsqlCommand(string.Join(";",
                    @"SELECT n.id,
                    @factor*distanceInMeters(latitude,longitude,@fromLatitude,@fromLongitude)
                    FROM node n JOIN edge e ON n.id=e.from_node OR n.id=e.to_node WHERE n.guid=@guid AND e.guid=@guid
                    AND @factor*distanceInMeters(latitude,longitude,@fromLatitude,@fromLongitude)<=@maxWeight
                    AND (e.from_node=@node OR e.to_node=@node)",@"SELECT id,from_node,to_node,
                    GREATEST((weight->@profile)::real,@minWeight),(direction->@profile)::smallint
                    FROM edge WHERE weight?@profile AND direction?@profile AND guid=@guid
                    AND (from_node=@node OR to_node=@node)"),
                    connection2);

            commandInsertIntoNode.Parameters.Add("id", SqliteType.Integer);
            commandInsertIntoNode.Parameters.Add("fromWeight", SqliteType.Real);
            commandInsertIntoNode.Prepare();

            commandInsertIntoEdge.Parameters.Add("id", SqliteType.Integer);
            commandInsertIntoEdge.Parameters.Add("fromNode", SqliteType.Integer);
            commandInsertIntoEdge.Parameters.Add("toNode", SqliteType.Integer);
            commandInsertIntoEdge.Parameters.Add("weight", SqliteType.Real);
            commandInsertIntoEdge.Parameters.Add("direction", SqliteType.Integer);
            commandInsertIntoEdge.Prepare();

            commandSelectFromNode.Parameters.AddWithValue("fromLatitude", source.Coordinate.Latitude);
            commandSelectFromNode.Parameters.AddWithValue("fromLongitude", source.Coordinate.Longitude);
            commandSelectFromNode.Parameters.AddWithValue("maxWeight", maxWeight);
            commandSelectFromNode.Parameters.AddWithValue("minWeight", minWeight);
            commandSelectFromNode.Parameters.AddWithValue("profile", Profile);
            commandSelectFromNode.Parameters.AddWithValue("factor", MinFactor);
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
                        commandInsertIntoNode.Parameters["fromWeight"].Value = reader.GetFloat(1);

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
                new SqliteCommand(@"REPLACE INTO temp_dijkstra (
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
	                0,
	                1
                )", connection))
            {
                command.Parameters.Add("node", SqliteType.Integer);
                command.Parameters.Add("latitude1", SqliteType.Real);
                command.Parameters.Add("longitude1", SqliteType.Real);
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
                new SqliteCommand(string.Join(";",
                    @"SELECT node,weight,NOT in_queue FROM temp_dijkstra WHERE node=@sourceFirst OR node=@sourceLast ORDER BY weight LIMIT 1",
                    @"SELECT node FROM temp_dijkstra WHERE in_queue ORDER BY weight LIMIT 1"),
                    connection))
            using (var command2 =
                new SqliteCommand(string.Join(";", @"INSERT INTO temp_dijkstra (
	                    node,
	                    weight,
	                    g,
	                    edge,
	                    in_queue
                    )
                    WITH cte AS
                    (
	                    SELECT *,ROW_NUMBER() OVER (PARTITION BY node ORDER BY weight) AS rn FROM (
		                    SELECT e.from_node AS node,n.from_weight+t.g+e.weight AS weight,t.g+e.weight AS g,e.id AS edge,1 AS in_queue
		                    FROM temp_edge e JOIN temp_node n ON e.from_node=n.id JOIN temp_dijkstra t ON e.to_node=t.node
                            WHERE (e.direction=0 OR e.direction=1 OR e.direction=3 OR e.direction=4) AND t.node=@node
                            AND NOT EXISTS (SELECT * FROM temp_restriction r 
                            JOIN temp_restriction_via_node vn ON vn.node=t.node AND r.id=vn.rid
                            JOIN temp_restriction_from_edge rf ON rf.edge=e.id AND r.id=rf.rid
                            JOIN temp_restriction_to_edge rt ON rt.edge=t.edge AND r.id=rt.rid)
                            UNION ALL SELECT e.to_node AS node,n.from_weight+t.g+e.weight AS weight,t.g+e.weight AS g,e.id AS edge,1 AS in_queue
		                    FROM temp_edge e JOIN temp_node n ON e.to_node=n.id JOIN temp_dijkstra t ON e.from_node=t.node
                            WHERE (e.direction=0 OR e.direction=2 OR e.direction=3 OR e.direction=5) AND t.node=@node
                            AND NOT EXISTS (SELECT * FROM temp_restriction r 
                            JOIN temp_restriction_via_node vn ON vn.node=t.node AND r.id=vn.rid
                            JOIN temp_restriction_from_edge rf ON rf.edge=e.id AND r.id=rf.rid
                            JOIN temp_restriction_to_edge rt ON rt.edge=t.edge AND r.id=rt.rid)) q
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
                    @"UPDATE temp_dijkstra SET in_queue=0 WHERE node=@node",
                    @"DELETE FROM temp_dijkstra WHERE weight>@maxWeight"), connection))
            {
                command.Parameters.AddWithValue("sourceFirst", sources.First());
                command.Parameters.AddWithValue("sourceLast", sources.Last());
                command2.Parameters.Add("maxWeight", SqliteType.Real);
                command2.Parameters.Add("node", SqliteType.Integer);
                command.Prepare();
                command2.Prepare();

                for (var step = 0L;; step++)
                {
                    var node1 = 0L;

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

                        reader.NextResult();

                        if (!reader.Read()) break;
                        node1 = reader.GetInt64(0);
                    }

                    LoadEdgesAndNodes(node1);

                    command2.Parameters["node"].Value = node1;
                    command2.Parameters["maxWeight"].Value = maxWeight;

                    command2.ExecuteNonQuery();

                    if (step % 100 == 0)
                        Console.WriteLine($"{DateTime.Now:O} Step {step} complete" +
                                          $" temp_dijkstra={new SqliteCommand("SELECT COUNT(*) FROM temp_dijkstra WHERE in_queue", connection).ExecuteScalar()}" +
                                          $" MIN(weight)={new SqliteCommand("SELECT MIN(weight) FROM temp_dijkstra WHERE in_queue", connection).ExecuteScalar()}" +
                                          $" MAX(weight)={new SqliteCommand("SELECT MAX(weight) FROM temp_dijkstra WHERE in_queue", connection).ExecuteScalar()}");
                }
            }

            using (var command =
                new SqliteCommand(@"SELECT e.from_node,e.id 
                    FROM temp_dijkstra t JOIN temp_edge e ON t.edge=e.id
                    WHERE t.node=@node AND e.to_node=t.node
                    UNION ALL SELECT e.to_node,e.id 
                    FROM temp_dijkstra t JOIN temp_edge e ON t.edge=e.id
                    WHERE t.node=@node AND e.from_node=t.node", connection))
            {
                command.Parameters.Add("node", SqliteType.Integer);
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