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
    public class InMemoryBidirectionalAStar : BasePathFinderAlgorithm
    {
        public InMemoryBidirectionalAStar(Guid guid, string connectionString, string vehicleType, string profile,
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
                )", @"CREATE TEMP TABLE temp_dijkstra1 (
	                node INTEGER PRIMARY KEY NOT NULL, 
	                weight REAL NOT NULL, 
	                weight1 REAL NOT NULL, 
	                edge INTEGER NOT NULL,
	                in_queue INTEGER NOT NULL
                )", @"CREATE TEMP TABLE temp_dijkstra2 (
	                node INTEGER PRIMARY KEY NOT NULL, 
	                weight REAL NOT NULL, 
	                weight1 REAL NOT NULL, 
	                edge INTEGER NOT NULL,
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
	                id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, 
	                rid INTEGER NOT NULL, 
	                edge INTEGER NOT NULL,
                    FOREIGN KEY(rid) REFERENCES shared_restriction(id)
                )", @"CREATE TEMP TABLE shared_restriction_to_edge (
	                id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, 
	                rid INTEGER NOT NULL, 
	                edge INTEGER NOT NULL,
                    FOREIGN KEY(rid) REFERENCES shared_restriction(id)
                )", @"CREATE TEMP TABLE shared_restriction_via_node (
	                id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, 
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
                @"INSERT INTO shared_edge (id,from_node,to_node,
                weight,direction) VALUES (@id,@fromNode,@toNode,
                    @weight,@direction)",
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
                new SqliteCommand(@"REPLACE INTO temp_dijkstra1 (
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
            var count1 = 0L;
            var count2 = 0L;

            using (var command =
                new SqliteCommand(
                    string.Join(";", @"SELECT COUNT(*) FROM temp_dijkstra1 WHERE in_queue",
                        @"SELECT COUNT(*) FROM temp_dijkstra2 WHERE in_queue"),
                    connection))
            using (var command1 =
                new SqliteCommand(
                    string.Join(";",
                        @"SELECT node FROM temp_dijkstra1 WHERE in_queue ORDER BY weight LIMIT 1"),
                    connection))
            using (var command2 =
                new SqliteCommand(
                    string.Join(";",
                        @"SELECT node FROM temp_dijkstra2 WHERE in_queue ORDER BY weight LIMIT 1"),
                    connection))
            using (var command3 =
                new SqliteCommand(string.Join(";", @"INSERT INTO temp_dijkstra1 (
	                    node,
	                    weight,
	                    weight1,
	                    edge,
	                    in_queue
                    )
                    WITH cte AS
                    (
	                    SELECT *,ROW_NUMBER() OVER (PARTITION BY node ORDER BY weight) AS rn FROM (
		                    SELECT e.to_node AS node,n.to_weight+t.weight1+e.weight AS weight,t.weight1+e.weight AS weight1,e.id AS edge,1 AS in_queue
		                    FROM shared_edge e JOIN temp_node n ON e.to_node=n.id JOIN temp_dijkstra1 t ON e.from_node=t.node
                            WHERE (e.direction=0 OR e.direction=1 OR e.direction=3 OR e.direction=4) AND t.node=@node
                            AND NOT EXISTS (SELECT * FROM shared_restriction r 
                            JOIN shared_restriction_via_node vn ON vn.node=t.node AND r.id=vn.rid
                            JOIN shared_restriction_to_edge rt ON rt.edge=e.id AND r.id=rt.rid
                            JOIN shared_restriction_from_edge rf ON rf.edge=t.edge AND r.id=rf.rid)
                            UNION ALL SELECT e.from_node AS node,n.to_weight+t.weight1+e.weight AS weight,t.weight1+e.weight AS weight1,e.id AS edge,1 AS in_queue
		                    FROM shared_edge e JOIN temp_node n ON e.from_node=n.id JOIN temp_dijkstra1 t ON e.to_node=t.node
                            WHERE (e.direction=0 OR e.direction=2 OR e.direction=3 OR e.direction=5) AND t.node=@node
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
                        WHERE temp_dijkstra1.weight>EXCLUDED.weight",
                    @"UPDATE temp_dijkstra1 SET in_queue=0 WHERE node=@node",
                    @"DELETE FROM temp_dijkstra1 WHERE weight>@maxWeight"), connection))
            using (var command4 =
                new SqliteCommand(string.Join(";", @"INSERT INTO temp_dijkstra2 (
	                    node,
	                    weight,
	                    weight1,
	                    edge,
	                    in_queue
                    )
                    WITH cte AS
                    (
	                    SELECT *,ROW_NUMBER() OVER (PARTITION BY node ORDER BY weight) AS rn FROM (
		                    SELECT e.from_node AS node,n.from_weight+t.weight1+e.weight AS weight,t.weight1+e.weight AS weight1,e.id AS edge,1 AS in_queue
		                    FROM shared_edge e JOIN temp_node n ON e.from_node=n.id JOIN temp_dijkstra2 t ON e.to_node=t.node
                            WHERE (e.direction=0 OR e.direction=1 OR e.direction=3 OR e.direction=4) AND t.node=@node
                            AND NOT EXISTS (SELECT * FROM shared_restriction r 
                            JOIN shared_restriction_via_node vn ON vn.node=t.node AND r.id=vn.rid
                            JOIN shared_restriction_to_edge rt ON rt.edge=e.id AND r.id=rt.rid
                            JOIN shared_restriction_from_edge rf ON rf.edge=t.edge AND r.id=rf.rid)
                            UNION ALL SELECT e.to_node AS node,n.from_weight+t.weight1+e.weight AS weight,t.weight1+e.weight AS weight1,e.id AS edge,1 AS in_queue
		                    FROM shared_edge e JOIN temp_node n ON e.to_node=n.id JOIN temp_dijkstra2 t ON e.from_node=t.node
                            WHERE (e.direction=0 OR e.direction=2 OR e.direction=3 OR e.direction=5) AND t.node=@node
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
                        WHERE temp_dijkstra2.weight>EXCLUDED.weight",
                    @"UPDATE temp_dijkstra2 SET in_queue=0 WHERE node=@node",
                    @"DELETE FROM temp_dijkstra2 WHERE weight>@maxWeight"), connection))
            using (var command5 =
                new SqliteCommand(string.Join(";",
                        @"SELECT t1.node,t1.weight1+t2.weight1,NOT t1.in_queue AND NOT t2.in_queue FROM temp_dijkstra1 t1
                JOIN temp_dijkstra2 t2 ON t1.node=t2.node WHERE NOT EXISTS (SELECT * FROM shared_restriction r 
                JOIN shared_restriction_via_node vn ON vn.node=t1.node AND r.id=vn.rid
                JOIN shared_restriction_to_edge rt ON rt.edge=t2.edge AND r.id=rt.rid
                JOIN shared_restriction_from_edge rf ON rf.edge=t1.edge AND r.id=rf.rid)
                ORDER BY t1.weight1+t2.weight1 LIMIT 1"),
                    connection))
            {
                command3.Parameters.Add("node", SqliteType.Integer);
                command3.Parameters.Add("maxWeight", SqliteType.Real);
                command4.Parameters.Add("node", SqliteType.Integer);
                command4.Parameters.Add("maxWeight", SqliteType.Real);
                command.Prepare();
                command1.Prepare();
                command2.Prepare();
                command3.Prepare();
                command4.Prepare();
                command5.Prepare();

                for (var step = 0L;; step++)
                {
                    using (var reader = command.ExecuteReader())
                    {
                        reader.Read();
                        count1 = reader.GetInt64(0);
                        reader.NextResult();
                        reader.Read();
                        count2 = reader.GetInt64(0);
                        reader.NextResult();
                    }

                    if (count1 > 0 && (count2 == 0 || count1 <= count2))
                    {
                        var node1 = (long) command1.ExecuteScalar();
                        command3.Parameters["node"].Value = node1;
                        command3.Parameters["maxWeight"].Value = maxWeight;
                        command3.ExecuteNonQuery();
                    }
                    else if (count2 > 0 && (count1 == 0 || count2 <= count1))
                    {
                        var node2 = (long)command2.ExecuteScalar();
                        command4.Parameters["node"].Value = node2;
                        command4.Parameters["maxWeight"].Value = maxWeight;
                        command4.ExecuteNonQuery();
                    }
                    else
                    {
                        Debug.Assert(count1 + count2 == 0);
                        break;
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

                    if (step % 100 == 0)
                        Console.WriteLine($"{DateTime.Now:O} Step {step} complete " +
                                          $" temp_dijkstra1={new SqliteCommand("SELECT COUNT(*) FROM temp_dijkstra1 WHERE in_queue", connection).ExecuteScalar()}" +
                                          $" temp_dijkstra2={new SqliteCommand("SELECT COUNT(*) FROM temp_dijkstra2 WHERE in_queue", connection).ExecuteScalar()}" +
                                          $" maxWeight={maxWeight}");
                }
            }

            var list = new List<long>();
            using (var command =
                new SqliteCommand(@"SELECT e.from_node,e.id 
                    FROM temp_dijkstra1 t JOIN shared_edge e ON t.edge=e.id
                    WHERE t.node=@node AND e.to_node=t.node
                    UNION ALL SELECT e.to_node,e.id 
                    FROM temp_dijkstra1 t JOIN shared_edge e ON t.edge=e.id
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
                    var edge = reader.GetInt64(1);
                    list.Add(edge);
                }
            }

            list.Reverse();

            using (var command =
                new SqliteCommand(@"SELECT e.from_node,e.id 
                    FROM temp_dijkstra2 t JOIN shared_edge e ON t.edge=e.id
                    WHERE t.node=@node AND e.to_node=t.node
                    UNION ALL SELECT e.to_node,e.id 
                    FROM temp_dijkstra2 t JOIN shared_edge e ON t.edge=e.id
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
                Weight = maxWeight
            };
        }
    }
}