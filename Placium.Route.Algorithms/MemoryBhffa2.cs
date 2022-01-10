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
    public class MemoryBhffa2 : BasePathFinderAlgorithm
    {
        public MemoryBhffa2(Guid guid, string connectionString, string vehicleType, string profile,
            float minFactor, float maxFactor) :
            base(guid, connectionString, vehicleType, profile, minFactor, maxFactor)
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
	                latitude REAL NOT NULL, 
	                longitude REAL NOT NULL
                )", @"CREATE TEMP TABLE temp_edge (
	                id INTEGER PRIMARY KEY NOT NULL, 
	                from_node INTEGER NOT NULL, 
	                to_node INTEGER NOT NULL,
	                weight REAL NOT NULL,
                    direction INTEGER NOT NULL
                )", @"CREATE TEMP TABLE temp_dijkstra1 (
	                node INTEGER PRIMARY KEY NOT NULL, 
	                weight1 REAL NOT NULL, 
	                edge INTEGER NULL,
                    FOREIGN KEY(node) REFERENCES temp_node(id),
                    FOREIGN KEY(edge) REFERENCES temp_edge(id)
                )", @"CREATE TEMP TABLE temp_dijkstra2 (
	                node INTEGER PRIMARY KEY NOT NULL, 
	                weight2 REAL NOT NULL, 
	                edge INTEGER NULL,
                    FOREIGN KEY(node) REFERENCES temp_node(id),
                    FOREIGN KEY(edge) REFERENCES temp_edge(id)
                )", @"CREATE TEMP TABLE temp_bhffa2  (
	                x INTEGER NOT NULL, 
	                y INTEGER NOT NULL, 
	                weight REAL NOT NULL, 
	                weight1 REAL NOT NULL, 
	                weight2 REAL NOT NULL, 
	                weight3 REAL NOT NULL,
	                in_queue INTEGER NOT NULL,
                    PRIMARY KEY (x,y),
                    FOREIGN KEY(x) REFERENCES temp_dijkstra1(node),
                    FOREIGN KEY(y) REFERENCES temp_dijkstra2(node)
                )", @"CREATE TEMP TABLE temp_restriction (
	                id INTEGER PRIMARY KEY NOT NULL
                )", @"CREATE TEMP TABLE temp_restriction_from_edge (
	                id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, 
	                rid INTEGER NOT NULL, 
	                edge INTEGER NOT NULL,
                    FOREIGN KEY(rid) REFERENCES temp_restriction(id)
                )", @"CREATE TEMP TABLE temp_restriction_to_edge (
	                id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, 
	                rid INTEGER NOT NULL, 
	                edge INTEGER NOT NULL,
                    FOREIGN KEY(rid) REFERENCES temp_restriction(id)
                )", @"CREATE TEMP TABLE temp_restriction_via_node (
	                id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, 
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
                        @"CREATE INDEX temp_bhffa2_in_queue_idx ON temp_bhffa2 (in_queue)",
                        @"CREATE INDEX temp_bhffa2_weight_idx ON temp_bhffa2 (weight)",
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
                @"INSERT INTO temp_node (id,latitude,longitude) VALUES (@id,@latitude,@longitude)
                ON CONFLICT (id) DO NOTHING",
                connection);
            using var commandInsertIntoEdge = new SqliteCommand(
                @"INSERT INTO temp_edge (id,from_node,to_node,
                weight,direction) VALUES (@id,@fromNode,@toNode,
                    @weight,@direction)
                ON CONFLICT (from_node,to_node) DO NOTHING",
                connection);
            using var commandSelectFromNode =
                new NpgsqlCommand(string.Join(";", @"SELECT n.id,latitude,longitude
                    FROM node n JOIN edge e ON n.id=e.to_node WHERE n.guid=@guid AND e.guid=@guid
                    AND e.from_node=@node UNION ALL SELECT n.id,latitude,longitude
                    FROM node n JOIN edge e ON n.id=e.from_node WHERE n.guid=@guid AND e.guid=@guid
                    AND e.to_node=@node", @"SELECT id,from_node,to_node,
                    GREATEST((weight->@profile)::real,@minWeight),(direction->@profile)::smallint
                    FROM edge WHERE weight?@profile AND direction?@profile AND guid=@guid
                    AND (from_node=@node OR to_node=@node)"),
                    connection2);

            commandInsertIntoNode.Parameters.Add("id", SqliteType.Integer);
            commandInsertIntoNode.Parameters.Add("latitude", SqliteType.Real);
            commandInsertIntoNode.Parameters.Add("longitude", SqliteType.Real);
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
                        commandInsertIntoNode.Parameters["latitude"].Value = reader.GetFloat(1);
                        commandInsertIntoNode.Parameters["longitude"].Value = reader.GetFloat(2);

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
                    weight1
                )
                VALUES (
	                @node,
	                0
                )", connection))
            {
                command.Parameters.Add("node", SqliteType.Integer);
                command.Prepare();

                if (new[] {0, 1, 3, 4}.Contains(source.Direction))
                {
                    LoadEdgesAndNodes(source.ToNode);
                    command.Parameters["node"].Value = source.ToNode;
                    await command.ExecuteNonQueryAsync();
                }

                if (new[] {0, 2, 3, 5}.Contains(source.Direction))
                {
                    LoadEdgesAndNodes(source.FromNode);
                    command.Parameters["node"].Value = source.FromNode;
                    await command.ExecuteNonQueryAsync();
                }
            }

            using (var command =
                new SqliteCommand(@"REPLACE INTO temp_dijkstra2 (
	                node,
                    weight2
                )
                VALUES (
	                @node,
	                0
                )", connection))
            {
                command.Parameters.Add("node", SqliteType.Integer);
                command.Prepare();

                if (new[] {0, 1, 3, 4}.Contains(target.Direction))
                {
                    LoadEdgesAndNodes(target.FromNode);
                    command.Parameters["node"].Value = target.FromNode;
                    await command.ExecuteNonQueryAsync();
                }

                if (new[] {0, 2, 3, 5}.Contains(target.Direction))
                {
                    LoadEdgesAndNodes(target.ToNode);
                    command.Parameters["node"].Value = target.ToNode;
                    await command.ExecuteNonQueryAsync();
                }
            }

            using (var command =
                new SqliteCommand(
                    string.Join(";", @"INSERT INTO temp_bhffa2 (x,y,weight,weight1,weight2,weight3,in_queue)
                    WITH cte AS (SELECT t1.node AS x,t2.node AS y,t1.weight1,t2.weight2,
                    @factor*distanceInMeters(n1.latitude,n1.longitude,n2.latitude,n2.longitude) AS weight3
                    FROM temp_dijkstra1 t1 JOIN temp_node n1 ON t1.node=n1.id,temp_dijkstra2 t2 JOIN temp_node n2 ON t2.node=n2.id)
                    SELECT x,y,weight1+weight2+weight3,weight1,weight2,weight3,1 AS in_queue FROM cte WHERE true
                    ON CONFLICT (x,y) DO NOTHING"),
                    connection))
            {
                command.Parameters.AddWithValue("factor", MinFactor);
                command.Prepare();
                await command.ExecuteNonQueryAsync();
            }

            var x = 0L;
            var y = 0L;
            var x1 = 0L;
            var y1 = 0L;
            float weight1;
            float weight2;
            long? edge = null;
            float? weight = null;

            using (var commandSelectFromQueue =
                new SqliteCommand(
                    string.Join(";",
                        @"SELECT x,y,weight1,weight2 FROM temp_bhffa2 WHERE in_queue AND weight1+weight2<=@maxWeight ORDER BY weight LIMIT 1"),
                    connection))
            using (var commandRemoveFromQueue =
                new SqliteCommand(string.Join(";", @"UPDATE temp_bhffa2 SET in_queue=0 WHERE x=@x AND y=@y"),
                    connection))
            using (var commandIfNodeIsSolution =
                new SqliteCommand(
                    string.Join(";", @"SELECT COUNT(*)
		                    FROM temp_dijkstra1 t1 JOIN temp_dijkstra2 t2 ON t1.node=t2.node
                            WHERE t1.node=@x
                            AND NOT EXISTS (SELECT * FROM temp_restriction r 
                            JOIN temp_restriction_via_node vn ON vn.node=t1.node AND r.id=vn.rid
                            JOIN temp_restriction_to_edge rt ON rt.edge=t2.edge AND r.id=rt.rid
                            JOIN temp_restriction_from_edge rf ON rf.edge=t1.edge AND r.id=rf.rid)"),
                    connection))
            using (var commandIfEdgeIsSolution =
                new SqliteCommand(
                    string.Join(";", @"WITH cte AS (
                            SELECT id,weight FROM temp_edge WHERE (direction=0 OR direction=1 OR direction=3 OR direction=4)
                            AND from_node=@x AND to_node=@y
                            UNION ALL SELECT id,weight FROM temp_edge WHERE (direction=0 OR direction=2 OR direction=3 OR direction=5)
                            AND to_node=@x AND from_node=@y)
                            SELECT cte.id,cte.weight FROM cte,temp_dijkstra1 t1,temp_dijkstra2 t2
                            WHERE t1.node=@x AND t2.node=@y
                            AND NOT EXISTS (SELECT * FROM temp_restriction r 
                            JOIN temp_restriction_via_node vn ON vn.node=@x AND r.id=vn.rid
                            JOIN temp_restriction_to_edge rt ON rt.edge=cte.id AND r.id=rt.rid
                            JOIN temp_restriction_from_edge rf ON rf.edge=t1.edge AND r.id=rf.rid)
                            AND NOT EXISTS (SELECT * FROM temp_restriction r 
                            JOIN temp_restriction_via_node vn ON vn.node=@y AND r.id=vn.rid
                            JOIN temp_restriction_to_edge rt ON rt.edge=t2.edge AND r.id=rt.rid
                            JOIN temp_restriction_from_edge rf ON rf.edge=cte.id AND r.id=rf.rid)
                            ORDER BY weight LIMIT 1"),
                    connection))
            using (var commandStep =
                new SqliteCommand(string.Join(";",
                    @"INSERT INTO temp_dijkstra1 (
	                    node,
	                    weight1,
	                    edge
                    )
                    WITH cte1 AS
                    (
	                    SELECT *,ROW_NUMBER() OVER (PARTITION BY node ORDER BY weight1) AS rn FROM (
		                    SELECT e.to_node AS node,t.weight1+e.weight AS weight1,e.id AS edge
		                    FROM temp_edge e JOIN temp_dijkstra1 t ON e.from_node=t.node
                            WHERE (e.direction=0 OR e.direction=1 OR e.direction=3 OR e.direction=4) AND t.node=@x
                            AND NOT EXISTS (SELECT * FROM temp_restriction r 
                            JOIN temp_restriction_via_node vn ON vn.node=t.node AND r.id=vn.rid
                            JOIN temp_restriction_to_edge rt ON rt.edge=e.id AND r.id=rt.rid
                            JOIN temp_restriction_from_edge rf ON rf.edge=t.edge AND r.id=rf.rid)
                            UNION ALL SELECT e.from_node AS node,t.weight1+e.weight AS weight1,e.id AS edge
		                    FROM temp_edge e JOIN temp_dijkstra1 t ON e.to_node=t.node
                            WHERE (e.direction=0 OR e.direction=2 OR e.direction=3 OR e.direction=5) AND t.node=@x
                            AND NOT EXISTS (SELECT * FROM temp_restriction r 
                            JOIN temp_restriction_via_node vn ON vn.node=t.node AND r.id=vn.rid
                            JOIN temp_restriction_to_edge rt ON rt.edge=e.id AND r.id=rt.rid
                            JOIN temp_restriction_from_edge rf ON rf.edge=t.edge AND r.id=rf.rid)) q
                    )
                    SELECT 
	                    node,
	                    weight1,
	                    edge
                    FROM cte1
                    WHERE rn = 1 AND weight1<@maxWeight
                    ON CONFLICT (node) DO UPDATE SET
	                    weight1=EXCLUDED.weight1,
	                    edge=EXCLUDED.edge 
                    WHERE temp_dijkstra1.weight1>EXCLUDED.weight1", @"INSERT INTO temp_dijkstra2 (
	                    node,
	                    weight2,
	                    edge
                    )
                    WITH cte2 AS
                    (
	                    SELECT *,ROW_NUMBER() OVER (PARTITION BY node ORDER BY weight2) AS rn FROM (
		                    SELECT e.from_node AS node,t.weight2+e.weight AS weight2,e.id AS edge
		                    FROM temp_edge e JOIN temp_dijkstra2 t ON e.to_node=t.node
                            WHERE (e.direction=0 OR e.direction=1 OR e.direction=3 OR e.direction=4) AND t.node=@y
                            AND NOT EXISTS (SELECT * FROM temp_restriction r 
                            JOIN temp_restriction_via_node vn ON vn.node=t.node AND r.id=vn.rid
                            JOIN temp_restriction_from_edge rt ON rt.edge=e.id AND r.id=rt.rid
                            JOIN temp_restriction_to_edge rf ON rf.edge=t.edge AND r.id=rf.rid)
                            UNION ALL SELECT e.to_node AS node,t.weight2+e.weight AS weight2,e.id AS edge
		                    FROM temp_edge e JOIN temp_dijkstra2 t ON e.from_node=t.node
                            WHERE (e.direction=0 OR e.direction=2 OR e.direction=3 OR e.direction=5) AND t.node=@y
                            AND NOT EXISTS (SELECT * FROM temp_restriction r 
                            JOIN temp_restriction_via_node vn ON vn.node=t.node AND r.id=vn.rid
                            JOIN temp_restriction_from_edge rt ON rt.edge=e.id AND r.id=rt.rid
                            JOIN temp_restriction_to_edge rf ON rf.edge=t.edge AND r.id=rf.rid)) q
                    )
                    SELECT 
	                    node,
	                    weight2,
	                    edge
                    FROM cte2
                    WHERE rn = 1 AND weight2<@maxWeight
                    ON CONFLICT (node) DO UPDATE SET
	                    weight2=EXCLUDED.weight2,
	                    edge=EXCLUDED.edge
                    WHERE temp_dijkstra2.weight2>EXCLUDED.weight2",
                    @"INSERT INTO temp_bhffa2 (x,y,weight,weight1,weight2,weight3,in_queue)
                    WITH cte1 AS
                    (
	                    SELECT DISTINCT node FROM (
		                    SELECT e.to_node AS node
		                    FROM temp_edge e JOIN temp_dijkstra1 t ON e.from_node=t.node
                            WHERE (e.direction=0 OR e.direction=1 OR e.direction=3 OR e.direction=4) AND t.node=@x
                            AND NOT EXISTS (SELECT * FROM temp_restriction r 
                            JOIN temp_restriction_via_node vn ON vn.node=t.node AND r.id=vn.rid
                            JOIN temp_restriction_to_edge rt ON rt.edge=e.id AND r.id=rt.rid
                            JOIN temp_restriction_from_edge rf ON rf.edge=t.edge AND r.id=rf.rid)
                            UNION ALL SELECT e.from_node AS node
		                    FROM temp_edge e JOIN temp_dijkstra1 t ON e.to_node=t.node
                            WHERE (e.direction=0 OR e.direction=2 OR e.direction=3 OR e.direction=5) AND t.node=@x
                            AND NOT EXISTS (SELECT * FROM temp_restriction r 
                            JOIN temp_restriction_via_node vn ON vn.node=t.node AND r.id=vn.rid
                            JOIN temp_restriction_to_edge rt ON rt.edge=e.id AND r.id=rt.rid
                            JOIN temp_restriction_from_edge rf ON rf.edge=t.edge AND r.id=rf.rid)) q
                    ), cte2 AS
                    (
	                    SELECT DISTINCT node FROM (
		                    SELECT e.from_node AS node
		                    FROM temp_edge e JOIN temp_dijkstra2 t ON e.to_node=t.node
                            WHERE (e.direction=0 OR e.direction=1 OR e.direction=3 OR e.direction=4) AND t.node=@y
                            AND NOT EXISTS (SELECT * FROM temp_restriction r 
                            JOIN temp_restriction_via_node vn ON vn.node=t.node AND r.id=vn.rid
                            JOIN temp_restriction_from_edge rf ON rf.edge=e.id AND r.id=rf.rid
                            JOIN temp_restriction_to_edge rt ON rt.edge=t.edge AND r.id=rt.rid)
                            UNION ALL SELECT e.to_node AS node
		                    FROM temp_edge e JOIN temp_dijkstra2 t ON e.from_node=t.node
                            WHERE (e.direction=0 OR e.direction=2 OR e.direction=3 OR e.direction=5) AND t.node=@y
                            AND NOT EXISTS (SELECT * FROM temp_restriction r 
                            JOIN temp_restriction_via_node vn ON vn.node=t.node AND r.id=vn.rid
                            JOIN temp_restriction_from_edge rf ON rf.edge=e.id AND r.id=rf.rid
                            JOIN temp_restriction_to_edge rt ON rt.edge=t.edge AND r.id=rt.rid)) q
                    ), cte AS (SELECT t1.node AS x,t2.node AS y,t1.weight1,t2.weight2,
                    @factor*distanceInMeters(n1.latitude,n1.longitude,n2.latitude,n2.longitude) AS weight3
                    FROM cte1 JOIN temp_dijkstra1 t1 ON cte1.node=t1.node JOIN temp_node n1 ON t1.node=n1.id,
                        cte2 JOIN temp_dijkstra2 t2 ON cte2.node=t2.node JOIN temp_node n2 ON t2.node=n2.id)
                    SELECT x,y,weight1+weight2+weight3,weight1,weight2,weight3,1 AS in_queue FROM cte WHERE true
                    ON CONFLICT (x,y) DO UPDATE SET
	                    weight=EXCLUDED.weight,
	                    weight1=EXCLUDED.weight1,
	                    weight2=EXCLUDED.weight2,
	                    weight3=EXCLUDED.weight3,
	                    in_queue=EXCLUDED.in_queue
                    WHERE temp_bhffa2.weight1>EXCLUDED.weight1 OR temp_bhffa2.weight2>EXCLUDED.weight2",
                    @"DELETE FROM temp_bhffa2 WHERE weight1+weight2>@maxWeight",
                    @"DELETE FROM temp_dijkstra1 WHERE weight1>@maxWeight",
                    @"DELETE FROM temp_dijkstra2 WHERE weight2>@maxWeight"), connection))
            {
                commandSelectFromQueue.Parameters.Add("maxWeight", SqliteType.Real);
                commandRemoveFromQueue.Parameters.Add("x", SqliteType.Integer);
                commandRemoveFromQueue.Parameters.Add("y", SqliteType.Integer);
                commandIfNodeIsSolution.Parameters.Add("x", SqliteType.Integer);
                commandIfEdgeIsSolution.Parameters.Add("x", SqliteType.Integer);
                commandIfEdgeIsSolution.Parameters.Add("y", SqliteType.Integer);
                commandStep.Parameters.Add("x", SqliteType.Integer);
                commandStep.Parameters.Add("y", SqliteType.Integer);
                commandStep.Parameters.Add("maxWeight", SqliteType.Real);
                commandStep.Parameters.AddWithValue("factor", MinFactor);
                commandSelectFromQueue.Prepare();
                commandIfNodeIsSolution.Prepare();
                commandIfEdgeIsSolution.Prepare();
                commandStep.Prepare();

                for (var step = 0L;; step++)
                {
                    commandSelectFromQueue.Parameters["maxWeight"].Value = maxWeight;
                    using (var reader = commandSelectFromQueue.ExecuteReader())
                    {
                        if (!reader.Read()) break;
                        x = reader.GetInt64(0);
                        y = reader.GetInt64(1);
                        weight1 = reader.GetFloat(2);
                        weight2 = reader.GetFloat(3);
                    }

                    commandRemoveFromQueue.Parameters["x"].Value = x;
                    commandRemoveFromQueue.Parameters["y"].Value = y;
                    commandRemoveFromQueue.ExecuteNonQuery();

                    if (x == y)
                    {
                        LoadEdgesAndNodes(x);
                        commandIfNodeIsSolution.Parameters["x"].Value = x;
                        var count = (long) commandIfNodeIsSolution.ExecuteScalar();
                        if (count > 0)
                        {
                            x1 = x;
                            y1 = y;
                            edge = null;
                            weight = maxWeight = weight1 + weight2;
                            break;
                        }
                    }
                    else
                    {
                        LoadEdgesAndNodes(x);
                        LoadEdgesAndNodes(y);
                        commandIfEdgeIsSolution.Parameters["x"].Value = x;
                        commandIfEdgeIsSolution.Parameters["y"].Value = y;
                        using (var reader = commandIfEdgeIsSolution.ExecuteReader())
                        {
                            if (reader.Read())
                            {
                                x1 = x;
                                y1 = y;
                                edge = reader.GetInt64(0);
                                weight = maxWeight = weight1 + weight2 + reader.GetFloat(1);
                                break;
                            }
                        }
                    }

                    if (x != x1 && y != y1)
                    {
                        commandStep.Parameters["x"].Value = x;
                        commandStep.Parameters["y"].Value = y;
                        commandStep.Parameters["maxWeight"].Value = maxWeight;
                        commandStep.ExecuteNonQuery();
                    }

                    if (step % 100 == 0)
                        Console.WriteLine($"{DateTime.Now:O} Step {step} complete" +
                                          $" temp_bhffa2={new SqliteCommand("SELECT COUNT(*) FROM temp_bhffa2 WHERE in_queue", connection).ExecuteScalar()}" +
                                          $" weight1={weight1}" +
                                          $" weight2={weight2}" +
                                          $" maxWeight={maxWeight}");
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
                for (var node1 = x1;;)
                {
                    command.Parameters["node"].Value = node1;
                    using var reader = await command.ExecuteReaderAsync();

                    if (!reader.Read()) break;

                    node1 = reader.GetInt64(0);
                    list.Add(reader.GetInt64(1));
                }
            }

            list.Reverse();

            if (edge != null) list.Add(edge.Value);

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
                for (var node2 = y1;;)
                {
                    command.Parameters["node"].Value = node2;
                    using var reader = await command.ExecuteReaderAsync();

                    if (!reader.Read()) break;

                    node2 = reader.GetInt64(0);
                    list.Add(reader.GetInt64(1));
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