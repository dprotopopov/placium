using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using Npgsql;
using Placium.Route.Common;

namespace Placium.Route.Algorithms
{
    public class InMemoryUniversalAStar : BasePathFinderAlgorithm
    {
        public InMemoryUniversalAStar(Guid guid, string connectionString, string vehicleType, string profile,
            float factor) :
            base(guid, connectionString, vehicleType, profile, factor)
        {
        }

        public override async Task<PathFinderResult> FindPathAsync(RouterPoint source,
            RouterPoint target, float maxWeight = float.MaxValue)
        {
            var minWeight = Factor * 1;

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
                declare
                   dist real;
                begin
                   select ST_DistanceSphere(ST_MakePoint(lon1,lat1),ST_MakePoint(lon2,lat2)) 
                   into dist;
                   return dist;
                end
                $$"), connection2))
            {
                command.Prepare();
                await command.ExecuteNonQueryAsync();
            }

            using (var command =
                new SqliteCommand(string.Join(";", "PRAGMA synchronous = OFF",
                    @"CREATE TEMP TABLE temp_node (
	                id INTEGER PRIMARY KEY NOT NULL, 
	                from_weight REAL NOT NULL, 
	                to_weight REAL NOT NULL
                )", @"CREATE TEMP TABLE temp_path (
	                id INTEGER PRIMARY KEY AUTOINCREMENT, 
	                from_node INTEGER NOT NULL, 
                    to_node INTEGER NOT NULL, 
	                weight1 REAL NOT NULL, 
	                weight REAL NOT NULL,
                    left_path INTEGER NOT NULL,
                    right_path INTEGER NOT NULL,
	                left_edge INTEGER NOT NULL, 
                    right_edge INTEGER NOT NULL, 
                    in_queue INTEGER NOT NULL 
                )", @"CREATE TEMP TABLE temp_dijkstra1 (
	                node INTEGER PRIMARY KEY NOT NULL, 
	                weight REAL NOT NULL, 
	                weight1 REAL NOT NULL, 
	                path INTEGER NOT NULL,
	                left_edge INTEGER NOT NULL,
	                right_edge INTEGER NOT NULL,
	                in_queue INTEGER NOT NULL
                )", @"CREATE TEMP TABLE temp_dijkstra2 (
	                node INTEGER PRIMARY KEY NOT NULL, 
	                weight REAL NOT NULL, 
	                weight1 REAL NOT NULL, 
	                path INTEGER NOT NULL,
	                left_edge INTEGER NOT NULL,
	                right_edge INTEGER NOT NULL,
	                in_queue INTEGER NOT NULL
                )", @"CREATE TEMP TABLE shared_edge (
	                id INTEGER PRIMARY KEY NOT NULL, 
	                from_node INTEGER NOT NULL, 
	                to_node INTEGER NOT NULL, 
	                weight REAL NOT NULL,
                    direction INTEGER NOT NULL
                )", @"CREATE TEMP TABLE shared_restriction (
	                id INTEGER PRIMARY KEY NOT NULL
                )", @"CREATE TEMP TABLE shared_restriction_from_edge (
	                rid INTEGER NOT NULL, 
	                edge INTEGER NOT NULL,
                    FOREIGN KEY(rid) REFERENCES shared_restriction(id)
                )", @"CREATE TEMP TABLE shared_restriction_to_edge (
	                rid INTEGER NOT NULL, 
	                edge INTEGER NOT NULL,
                    FOREIGN KEY(rid) REFERENCES shared_restriction(id)
                )", @"CREATE TEMP TABLE shared_restriction_via_node (
	                rid INTEGER NOT NULL, 
	                node INTEGER NOT NULL,
                    FOREIGN KEY(rid) REFERENCES shared_restriction(id)
                )"), connection))
            {
                command.Prepare();
                await command.ExecuteNonQueryAsync();
            }

            using (var command =
                new SqliteCommand(@"BEGIN TRANSACTION",
                    connection))
            using (var command2 = new SqliteCommand(
                @"INSERT INTO temp_node (id,from_weight,to_weight) VALUES (@id,
                @fromWeight,
                @toWeight)",
                connection))
            using (var command3 =
                new SqliteCommand(@"COMMIT",
                    connection))
            using (var command4 =
                new NpgsqlCommand(
                    @"SELECT id,
                    @factor*distanceInMeters(latitude,longitude,@fromLatitude,@fromLongitude),
                    @factor*distanceInMeters(latitude,longitude,@toLatitude,@toLongitude)
                    FROM node WHERE is_core AND guid=@guid
                    AND @factor*(distanceInMeters(latitude,longitude,@fromLatitude,@fromLongitude)+
                    distanceInMeters(latitude,longitude,@toLatitude,@toLongitude))<=@maxWeight",
                    connection2))
            {
                command2.Parameters.Add("id", SqliteType.Integer);
                command2.Parameters.Add("fromWeight", SqliteType.Real);
                command2.Parameters.Add("toWeight", SqliteType.Real);
                command2.Prepare();

                command4.Parameters.AddWithValue("fromLatitude", source.Coordinate.Latitude);
                command4.Parameters.AddWithValue("fromLongitude", source.Coordinate.Longitude);
                command4.Parameters.AddWithValue("toLatitude", target.Coordinate.Latitude);
                command4.Parameters.AddWithValue("toLongitude", target.Coordinate.Longitude);
                command4.Parameters.AddWithValue("maxWeight", maxWeight);
                command4.Parameters.AddWithValue("factor", Factor);
                command4.Parameters.AddWithValue("guid", Guid);
                command4.Prepare();

                command.ExecuteNonQuery();

                using var reader = await command4.ExecuteReaderAsync();
                while (reader.Read())
                {
                    command2.Parameters["id"].Value = reader.GetInt64(0);
                    command2.Parameters["fromWeight"].Value = reader.GetFloat(1);
                    command2.Parameters["toWeight"].Value = reader.GetFloat(2);

                    command2.ExecuteNonQuery();
                }

                command3.ExecuteNonQuery();
            }


            using (var command = new SqliteCommand(
                @"INSERT INTO shared_edge (id,from_node,to_node,weight,direction) VALUES (@id,@fromNode,@toNode,@weight,@direction)",
                connection))
            using (var command2 =
                new NpgsqlCommand(
                    @"SELECT id,from_node,to_node,
                    GREATEST((weight->@profile)::real,@minWeight),(direction->@profile)::smallint
                    FROM edge WHERE (weight->@profile)::real>0 AND direction?@profile AND guid=@guid",
                    connection2))
            {
                command.Parameters.Add("id", SqliteType.Integer);
                command.Parameters.Add("fromNode", SqliteType.Integer);
                command.Parameters.Add("toNode", SqliteType.Integer);
                command.Parameters.Add("weight", SqliteType.Real);
                command.Parameters.Add("direction", SqliteType.Integer);
                command.Prepare();

                command2.Parameters.AddWithValue("minWeight", minWeight);
                command2.Parameters.AddWithValue("profile", Profile);
                command2.Parameters.AddWithValue("guid", Guid);
                command2.Prepare();

                using var reader2 = await command2.ExecuteReaderAsync();
                while (reader2.Read())
                {
                    command.Parameters["id"].Value = reader2.GetInt64(0);
                    command.Parameters["fromNode"].Value = reader2.GetInt64(1);
                    command.Parameters["toNode"].Value = reader2.GetInt64(2);
                    command.Parameters["weight"].Value = reader2.GetFloat(3);
                    command.Parameters["direction"].Value = reader2.GetInt16(4);

                    command.ExecuteNonQuery();
                }
            }


            using (var command =
                new SqliteCommand(@"BEGIN TRANSACTION",
                    connection))
            using (var command2 =
                new SqliteCommand(@"INSERT INTO shared_restriction(id) VALUES (@id)",
                    connection))
            using (var command3 =
                new SqliteCommand(@"INSERT INTO shared_restriction_from_edge(rid,edge) VALUES (@id,@edge)",
                    connection))
            using (var command4 =
                new SqliteCommand(@"INSERT INTO shared_restriction_to_edge(rid,edge) VALUES (@id,@edge)",
                    connection))
            using (var command5 =
                new SqliteCommand(@"INSERT INTO shared_restriction_via_node(rid,node) VALUES (@id,@node)",
                    connection))
            using (var command6 =
                new SqliteCommand(@"COMMIT",
                    connection))
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
                command2.Parameters.Add("id", SqliteType.Integer);
                command2.Prepare();

                command3.Parameters.Add("id", SqliteType.Integer);
                command3.Parameters.Add("edge", SqliteType.Integer);
                command3.Prepare();

                command4.Parameters.Add("id", SqliteType.Integer);
                command4.Parameters.Add("edge", SqliteType.Integer);
                command4.Prepare();

                command5.Parameters.Add("id", SqliteType.Integer);
                command5.Parameters.Add("node", SqliteType.Integer);
                command5.Prepare();

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

                await command.ExecuteNonQueryAsync();

                using (var reader = await command7.ExecuteReaderAsync())
                {
                    while (reader.Read())
                    {
                        command2.Parameters["id"].Value = reader.GetInt64(0);
                        command2.ExecuteNonQuery();
                    }
                }

                using (var reader = await command8.ExecuteReaderAsync())
                {
                    while (reader.Read())
                    {
                        command3.Parameters["id"].Value = reader.GetInt64(0);
                        command3.Parameters["edge"].Value = reader.GetInt64(1);
                        command3.ExecuteNonQuery();
                    }
                }

                using (var reader = await command9.ExecuteReaderAsync())
                {
                    while (reader.Read())
                    {
                        command4.Parameters["id"].Value = reader.GetInt64(0);
                        command4.Parameters["edge"].Value = reader.GetInt64(1);
                        command4.ExecuteNonQuery();
                    }
                }

                using (var reader = await command10.ExecuteReaderAsync())
                {
                    while (reader.Read())
                    {
                        command5.Parameters["id"].Value = reader.GetInt64(0);
                        command5.Parameters["node"].Value = reader.GetInt64(1);
                        command5.ExecuteNonQuery();
                    }
                }

                await command6.ExecuteNonQueryAsync();
            }

            using (var command =
                new SqliteCommand(string.Join(";",
                        @"CREATE UNIQUE INDEX temp_path_from_node_to_node_idx ON temp_path (from_node,to_node)",
                        @"CREATE INDEX temp_path_from_node_idx ON temp_path (from_node)",
                        @"CREATE INDEX temp_path_to_node_idx ON temp_path (to_node)",
                        @"CREATE INDEX temp_path_weight_idx ON temp_path (weight)",
                        @"CREATE INDEX temp_path_left_edge_idx ON temp_path (left_edge)",
                        @"CREATE INDEX temp_path_right_edge_idx ON temp_path (right_edge)",
                        @"CREATE INDEX temp_path_in_queue_idx ON temp_path (in_queue)",
                        @"CREATE INDEX temp_path_left_path_idx ON temp_path (left_path)",
                        @"CREATE INDEX temp_path_right_path_idx ON temp_path (right_path)",
                        @"CREATE INDEX temp_dijkstra1_in_queue_idx ON temp_dijkstra1 (in_queue)",
                        @"CREATE INDEX temp_dijkstra1_weight_idx ON temp_dijkstra1 (weight)",
                        @"CREATE INDEX temp_dijkstra2_in_queue_idx ON temp_dijkstra2 (in_queue)",
                        @"CREATE INDEX temp_dijkstra2_weight_idx ON temp_dijkstra2 (weight)",
                        @"CREATE UNIQUE INDEX shared_edge_from_node_to_node_idx ON shared_edge (from_node,to_node)",
                        @"CREATE INDEX shared_restriction_from_edge_idx ON shared_restriction_from_edge (edge)",
                        @"CREATE INDEX shared_restriction_to_edge_idx ON shared_restriction_to_edge (edge)",
                        @"CREATE INDEX shared_restriction_via_node_idx ON shared_restriction_via_node (node)"),
                    connection))
            {
                command.Prepare();
                await command.ExecuteNonQueryAsync();
            }

            using (var command =
                new SqliteCommand(@"INSERT INTO temp_path (
	                from_node,
                    to_node,
                    weight1,
                    weight,
                    left_edge,
                    right_edge,
	                left_path,
                    right_path,
                    in_queue
                )
                    WITH cte AS
                    (
	                    SELECT *,ROW_NUMBER() OVER (PARTITION BY from_node,to_node ORDER BY weight) AS rn FROM (
                            SELECT e.from_node,e.to_node,e.weight AS weight1,
                            nf.from_weight+e.weight+nt.to_weight AS weight,e.id AS left_edge,e.id AS right_edge,
                            0 AS left_path,0 AS right_path,1 AS in_queue
                            FROM shared_edge e JOIN temp_node nf ON e.from_node=nf.id JOIN temp_node nt ON e.to_node=nt.id
                            WHERE (e.direction=0 OR e.direction=1 OR e.direction=3 OR e.direction=4)
                            UNION SELECT e.to_node AS from_node,e.from_node AS to_node,e.weight AS weight1,
                            nf.from_weight+e.weight+nt.to_weight AS weight,e.id AS left_edge,e.id AS right_edge,
                            0 AS left_path,0 AS right_path,1 AS in_queue
                            FROM shared_edge e JOIN temp_node nf ON e.to_node=nf.id JOIN temp_node nt ON e.from_node=nt.id
                            WHERE (e.direction=0 OR e.direction=2 OR e.direction=3 OR e.direction=5)) q
                    )
                    SELECT 
	                    from_node,
                        to_node,
                        weight1,
	                    weight,
                        left_edge,
                        right_edge,
	                    left_path,
                        right_path,
                        in_queue
                    FROM cte
                    WHERE rn = 1 AND weight<@maxWeight", connection))
            {
                command.Parameters.AddWithValue("maxWeight", maxWeight);
                command.Prepare();
                await command.ExecuteNonQueryAsync();
            }

            using (var command =
                new SqliteCommand(@"REPLACE INTO temp_dijkstra1 (
	                node,
	                weight,
                    weight1,
	                path,
                    left_edge,
                    right_edge,
	                in_queue
                )
                VALUES (
	                @node,
                    @factor*distanceInMeters(@latitude,@longitude,@latitude1,@longitude1),
	                0,
	                0,
	                0,
	                0,
	                1
                )", connection))
            {
                command.Parameters.Add("node", SqliteType.Integer);
                command.Parameters.Add("latitude1", SqliteType.Real);
                command.Parameters.Add("longitude1", SqliteType.Real);
                command.Parameters.AddWithValue("latitude", target.Coordinate.Latitude);
                command.Parameters.AddWithValue("longitude", target.Coordinate.Longitude);
                command.Parameters.AddWithValue("factor", Factor);
                command.Prepare();

                if (new[] {0, 1, 3, 4}.Contains(source.Direction))
                {
                    command.Parameters["node"].Value = source.ToNode;
                    var coords = source.Coordinates.Last();
                    command.Parameters["latitude1"].Value = coords.Latitude;
                    command.Parameters["longitude1"].Value = coords.Longitude;
                    await command.ExecuteNonQueryAsync();
                }

                if (new[] {0, 2, 3, 5}.Contains(source.Direction))
                {
                    command.Parameters["node"].Value = source.FromNode;
                    var coords = source.Coordinates.First();
                    command.Parameters["latitude1"].Value = coords.Latitude;
                    command.Parameters["longitude1"].Value = coords.Longitude;
                    await command.ExecuteNonQueryAsync();
                }
            }

            using (var command =
                new SqliteCommand(@"REPLACE INTO temp_dijkstra2 (
	                node,
	                weight,
                    weight1,
	                path,
                    left_edge,
                    right_edge,
	                in_queue
                )
                VALUES (
	                @node,
                    @factor*distanceInMeters(@latitude,@longitude,@latitude1,@longitude1),
	                0,
	                0,
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
                command.Parameters.AddWithValue("factor", Factor);
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
            var limit = 10;

            using (var command =
                new SqliteCommand(
                    string.Join(";",
                        @"SELECT id FROM temp_path WHERE in_queue ORDER BY weight LIMIT 1"),
                    connection))
            using (var command1 =
                new SqliteCommand(
                    string.Join(";", @"SELECT node FROM temp_dijkstra1 WHERE in_queue ORDER BY weight LIMIT 1",
                        @"SELECT node FROM temp_dijkstra2 WHERE in_queue ORDER BY weight LIMIT 1"),
                    connection))
            using (var command2 =
                new SqliteCommand(string.Join("; ", @"INSERT INTO temp_path (
                        from_node,
                        to_node,
                        weight1,
                        weight,
                        left_edge,
                        right_edge,
                        left_path,
                        right_path,
                        in_queue
                    )
                    WITH cte AS (
                         SELECT *,ROW_NUMBER() OVER (PARTITION BY from_node,to_node ORDER BY weight) AS rn FROM (
                             SELECT l.from_node,r.to_node,l.weight1+r.weight1 AS weight1,
                                        nl.from_weight+l.weight1+r.weight1+nr.to_weight AS weight,
                                        l.left_edge,r.right_edge,
                                        l.id AS left_path,r.id AS right_path,1 AS in_queue
                                  FROM temp_path l JOIN temp_path r ON l.to_node=r.from_node
                                        JOIN temp_node nl ON l.from_node=nl.id JOIN temp_node nr ON r.to_node=nr.id
                                        WHERE l.id=@path AND l.from_node<>r.to_node AND NOT EXISTS (SELECT * FROM shared_restriction r 
                                        JOIN shared_restriction_via_node vn ON vn.node=l.to_node AND r.id=vn.rid
                                        JOIN shared_restriction_to_edge rt ON rt.edge=r.left_edge AND r.id=rt.rid
                                        JOIN shared_restriction_from_edge rf ON rf.edge=l.right_edge AND r.id=rf.rid)
                             UNION ALL SELECT l.from_node,r.to_node,l.weight1+r.weight1 AS weight1,
                                        nl.from_weight+l.weight1+r.weight1+nr.to_weight AS weight,
                                        l.left_edge,r.right_edge,
                                        l.id AS left_path,r.id AS right_path,1 AS in_queue
                                  FROM temp_path l JOIN temp_path r ON l.to_node=r.from_node
                                        JOIN temp_node nl ON l.from_node=nl.id JOIN temp_node nr ON r.to_node=nr.id
                                        WHERE r.id=@path AND l.from_node<>r.to_node AND NOT EXISTS (SELECT * FROM shared_restriction r 
                                        JOIN shared_restriction_via_node vn ON vn.node=l.to_node AND r.id=vn.rid
                                        JOIN shared_restriction_to_edge rt ON rt.edge=r.left_edge AND r.id=rt.rid
                                        JOIN shared_restriction_from_edge rf ON rf.edge=l.right_edge AND r.id=rf.rid)) q
                    )
                    SELECT 
                        from_node,
                        to_node,
                        weight1,
                        weight,
                        left_edge,
                        right_edge,
                        left_path,
                        right_path,
                        in_queue
                    FROM cte
                    WHERE rn = 1 AND weight<@maxWeight
                    ON CONFLICT (from_node,to_node) DO UPDATE SET 
                        weight=EXCLUDED.weight,
                        weight1=EXCLUDED.weight1,
                        left_path=EXCLUDED.left_path,
                        right_path=EXCLUDED.right_path,
                        in_queue=EXCLUDED.in_queue,
                        left_edge=EXCLUDED.left_edge,
                        right_edge=EXCLUDED.right_edge
                    WHERE temp_path.weight>EXCLUDED.weight",
                        @"UPDATE temp_path SET in_queue=0 WHERE id=@path"
                    ),
                    connection))
            using (var command3 =
                new SqliteCommand(string.Join("; ", @"INSERT INTO temp_dijkstra1 (
	                    node,
	                    weight,
	                    weight1,
	                    path,
                        left_edge,
                        right_edge,
	                    in_queue
                    )
                    WITH cte AS
                    (
	                    SELECT *,ROW_NUMBER() OVER (PARTITION BY node ORDER BY weight) AS rn FROM (
		                    SELECT p.to_node AS node,n.to_weight+t.weight1+p.weight1 AS weight,t.weight1+p.weight1 AS weight1,p.id AS path,
                            t.left_edge,p.right_edge,1 AS in_queue
		                    FROM temp_path p JOIN temp_dijkstra1 t ON p.from_node=t.node JOIN temp_node n ON p.to_node=n.id
                            WHERE t.node=@node AND NOT EXISTS (SELECT * FROM shared_restriction r 
                            JOIN shared_restriction_via_node vn ON vn.node=t.node AND r.id=vn.rid
                            JOIN shared_restriction_to_edge rt ON rt.edge=p.left_edge AND r.id=rt.rid
                            JOIN shared_restriction_from_edge rf ON rf.edge=t.right_edge AND r.id=rf.rid)) q
                    )
                    SELECT 
	                    node,
	                    weight,
	                    weight1,
	                    path,
                        left_edge,
                        right_edge,
	                    in_queue
                    FROM cte
                    WHERE rn = 1 AND weight<@maxWeight
                    ON CONFLICT (node) DO UPDATE SET
	                    weight=EXCLUDED.weight,
	                    weight1=EXCLUDED.weight1,
	                    path=EXCLUDED.path,
	                    left_edge=EXCLUDED.left_edge,
	                    right_edge=EXCLUDED.right_edge,
                        in_queue=EXCLUDED.in_queue
                    WHERE temp_dijkstra1.weight>EXCLUDED.weight",
                        @"UPDATE temp_dijkstra1 SET in_queue=0 WHERE node=@node"
                    ),
                    connection))
            using (var command4 =
                new SqliteCommand(string.Join("; ", @"INSERT INTO temp_dijkstra2 (
	                    node,
	                    weight,
	                    weight1,
	                    path,
                        left_edge,
                        right_edge,
	                    in_queue
                    )
                    WITH cte AS
                    (
	                    SELECT *,ROW_NUMBER() OVER (PARTITION BY node ORDER BY weight) AS rn FROM (
		                    SELECT p.from_node AS node,n.from_weight+t.weight1+p.weight1 AS weight,t.weight1+p.weight1 AS weight1,p.id AS path,
                            p.left_edge,t.right_edge,1 AS in_queue
		                    FROM temp_path p JOIN temp_dijkstra2 t ON p.to_node=t.node JOIN temp_node n ON p.from_node=n.id
                            WHERE t.node=@node AND NOT EXISTS (SELECT * FROM shared_restriction r 
                            JOIN shared_restriction_via_node vn ON vn.node=t.node AND r.id=vn.rid
                            JOIN shared_restriction_to_edge rt ON rt.edge=p.right_edge AND r.id=rt.rid
                            JOIN shared_restriction_from_edge rf ON rf.edge=t.left_edge AND r.id=rf.rid)) q
                    )
                    SELECT 
	                    node,
	                    weight,
	                    weight1,
	                    path,
                        left_edge,
                        right_edge,
	                    in_queue
                    FROM cte
                    WHERE rn = 1 AND weight<@maxWeight
                    ON CONFLICT (node) DO UPDATE SET
	                    weight=EXCLUDED.weight,
	                    weight1=EXCLUDED.weight1,
	                    path=EXCLUDED.path,
	                    left_edge=EXCLUDED.left_edge,
	                    right_edge=EXCLUDED.right_edge,
                        in_queue=EXCLUDED.in_queue
                    WHERE temp_dijkstra2.weight>EXCLUDED.weight",
                        @"UPDATE temp_dijkstra2 SET in_queue=0 WHERE node=@node"
                    ),
                    connection))
            using (var command5 =
                new SqliteCommand(string.Join(";",
                        @"SELECT t1.node,t1.weight1+t2.weight1,NOT t1.in_queue AND NOT t2.in_queue FROM temp_dijkstra1 t1
                JOIN temp_dijkstra2 t2 ON t1.node=t2.node WHERE NOT EXISTS (SELECT * FROM shared_restriction r 
                JOIN shared_restriction_via_node vn ON vn.node=t1.node AND r.id=vn.rid
                JOIN shared_restriction_to_edge rt ON rt.edge=t2.left_edge AND r.id=rt.rid
                JOIN shared_restriction_from_edge rf ON rf.edge=t1.right_edge AND r.id=rf.rid)
                ORDER BY t1.weight1+t2.weight1 LIMIT 1"),
                    connection))
            using (var command6 =
                new SqliteCommand(string.Join(";",
                        @"DELETE FROM temp_dijkstra1 WHERE weight>@maxWeight",
                        @"DELETE FROM temp_dijkstra2 WHERE weight>@maxWeight",
                        @"DELETE FROM temp_path WHERE weight>@maxWeight"),
                    connection))
            {
                command2.Parameters.Add("path", SqliteType.Integer);
                command3.Parameters.Add("node", SqliteType.Integer);
                command4.Parameters.Add("node", SqliteType.Integer);
                command2.Parameters.Add("maxWeight", SqliteType.Real);
                command3.Parameters.Add("maxWeight", SqliteType.Real);
                command4.Parameters.Add("maxWeight", SqliteType.Real);
                command6.Parameters.Add("maxWeight", SqliteType.Real);
                command1.Prepare();
                command2.Prepare();
                command5.Prepare();
                command6.Prepare();

                for (var step = 0L;; step++)
                {
                    var node1 = 0L;
                    var node2 = 0L;
                    var path = 0L;

                    for (var i = 0; i < limit; i++)
                    {
                        using (var reader = command.ExecuteReader())
                        {
                            path = reader.Read() ? reader.GetInt64(0) : 0L;
                        }

                        if (path == 0) break;

                        command2.Parameters["maxWeight"].Value = maxWeight;
                        command2.Parameters["path"].Value = path;
                        command2.ExecuteNonQuery();
                    }


                    using (var reader = command1.ExecuteReader())
                    {
                        node1 = reader.Read() ? reader.GetInt64(0) : 0L;
                        reader.NextResult();
                        node2 = reader.Read() ? reader.GetInt64(0) : 0L;
                    }

 
                    if (node1 > 0)
                    {
                        command3.Parameters["maxWeight"].Value = maxWeight;
                        command3.Parameters["node"].Value = node1;
                        command3.ExecuteNonQuery();
                    }

                    if (node2 > 0)
                    {
                        command4.Parameters["maxWeight"].Value = maxWeight;
                        command4.Parameters["node"].Value = node2;
                        command4.ExecuteNonQuery();
                    }

                    using (var reader = command5.ExecuteReader())
                    {
                        if (reader.Read())
                        {
                            node = reader.GetInt64(0);
                            maxWeight = reader.GetFloat(1);
                            if (reader.GetBoolean(2)) break;
                        }
                    }

                    command6.Parameters["maxWeight"].Value = maxWeight;
                    command6.ExecuteNonQuery();

                    if (step % 10 == 0)
                        Console.WriteLine($"{DateTime.Now:O} Step {step} complete " +
                                          $" temp_path={new SqliteCommand("SELECT COUNT(*) FROM temp_path", connection).ExecuteScalar()}" +
                                          $" temp_dijkstra1={new SqliteCommand("SELECT COUNT(*) FROM temp_dijkstra1 WHERE in_queue", connection).ExecuteScalar()}" +
                                          $" temp_dijkstra2={new SqliteCommand("SELECT COUNT(*) FROM temp_dijkstra2 WHERE in_queue", connection).ExecuteScalar()}" +
                                          $" temp_path={new SqliteCommand("SELECT MAX(weight) FROM temp_path", connection).ExecuteScalar()}" +
                                          $" temp_dijkstra1={new SqliteCommand("SELECT MAX(weight) FROM temp_dijkstra1", connection).ExecuteScalar()}" +
                                          $" temp_dijkstra2={new SqliteCommand("SELECT MAX(weight) FROM temp_dijkstra2", connection).ExecuteScalar()}" +
                                          $" maxWeight={maxWeight}");

                    if (node1 == 0 || node2 == 0) break;
                }
            }


            using (var command =
                new SqliteCommand(@"SELECT left_path,right_path,left_edge FROM temp_path WHERE id=@path",
                    connection))
            using (var command1 =
                new SqliteCommand(
                    @"SELECT p.from_node,t.path FROM temp_dijkstra1 t JOIN temp_path p ON p.id=t.path WHERE t.node=@node",
                    connection))
            using (var command2 =
                new SqliteCommand(
                    @"SELECT p.to_node,t.path FROM temp_dijkstra2 t JOIN temp_path p ON p.id=t.path WHERE t.node=@node",
                    connection))
            {
                command.Parameters.Add("path", SqliteType.Integer);
                command1.Parameters.Add("node", SqliteType.Integer);
                command2.Parameters.Add("node", SqliteType.Integer);
                command.Prepare();
                command1.Prepare();
                command2.Prepare();

                List<long> GetPathEdges(long path)
                {
                    command.Parameters["path"].Value = path;

                    long leftPath;
                    long rightPath;
                    long edge;
                    using (var reader = command.ExecuteReader())
                    {
                        if (!reader.Read()) return new List<long>();
                        leftPath = reader.GetInt64(0);
                        rightPath = reader.GetInt64(1);
                        edge = reader.GetInt64(2);
                    }

                    if (leftPath == 0 && rightPath == 0) return new List<long> {edge};
                    Debug.Assert(leftPath != 0 && rightPath != 0);
                    return GetPathEdges(leftPath).Union(GetPathEdges(rightPath)).ToList();
                }

                var list = new List<long>();
                var paths = new List<long>();

                for (var node1 = node;;)
                {
                    command1.Parameters["node"].Value = node1;
                    using (var reader = command1.ExecuteReader())
                    {
                        if (!reader.Read()) break;
                        node1 = reader.GetInt64(0);
                        paths.Add(reader.GetInt64(1));
                    }
                }

                paths.Reverse();

                for (var node2 = node;;)
                {
                    command2.Parameters["node"].Value = node2;
                    using (var reader = command2.ExecuteReader())
                    {
                        if (!reader.Read()) break;
                        node2 = reader.GetInt64(0);
                        paths.Add(reader.GetInt64(1));
                    }
                }

                foreach (var path in paths) list.AddRange(GetPathEdges(path));

                return new PathFinderResult
                {
                    Edges = list,
                    Weight = maxWeight
                };
            }
        }
    }
}