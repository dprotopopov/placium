﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using Npgsql;
using Placium.Route.Common;

namespace Placium.Route.Algorithms
{
    public class InMemoryAStar : BaseDatabaseAlgorithm<List<long>>
    {
        public InMemoryAStar(Guid guid, string connectionString, string vehicleType, string profile,
            RouterPoint source,
            RouterPoint target) : base(guid, connectionString, profile)
        {
            Source = source;
            Target = target;
            VehicleType = vehicleType;
        }

        public RouterPoint Source { get; }
        public RouterPoint Target { get; }
        public string VehicleType { get; }

        public override async Task<List<long>> DoRunAsync()
        {
            using var connection = new SqliteConnection("Data Source=:memory:");
            using var connection2 = new NpgsqlConnection(ConnectionString);
            await connection.OpenAsync();
            await connection2.OpenAsync();

            connection.CreateFunction(
                "radians",
                (double x) => Math.PI * x / 180);
            connection.CreateFunction<double, double>(
                "sin",
                Math.Sin);
            connection.CreateFunction<double, double>(
                "cos",
                Math.Cos);
            connection.CreateFunction<double, double>(
                "acos",
                Math.Acos);
            connection.CreateFunction<double, double, double>(
                "max",
                Math.Max);
            connection.CreateFunction<double, double, double>(
                "min",
                Math.Min);

            using (var command =
                new SqliteCommand(string.Join(";", @"CREATE TEMP TABLE temp_dijkstra (
	                node INTEGER PRIMARY KEY NOT NULL, 
	                weight REAL NOT NULL, 
	                weight1 REAL NOT NULL, 
	                edge INTEGER NOT NULL,
	                in_queue INTEGER NOT NULL
                )", @"CREATE TEMP TABLE temp_edge (
	                id INTEGER PRIMARY KEY NOT NULL, 
	                from_node INTEGER NOT NULL, 
	                to_node INTEGER NOT NULL,
	                from_latitude REAL NOT NULL, 
	                from_longitude REAL NOT NULL, 
	                to_latitude REAL NOT NULL, 
	                to_longitude REAL NOT NULL, 
	                weight REAL NOT NULL,
                    direction INTEGER NOT NULL,
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

            using (var command = new SqliteCommand(
                @"INSERT INTO temp_edge (id,from_node,to_node,
                    from_latitude, 
                    from_longitude,
                    to_latitude,
                    to_longitude,
                    weight,direction) VALUES (@id,@fromNode,@toNode,
                    @fromLatitude, 
                    @fromLongitude,
                    @toLatitude,
                    @toLongitude,
                    @weight,@direction)",
                connection))
            using (var command2 =
                new NpgsqlCommand(
                    @"SELECT id,from_node,to_node,
                    from_latitude, 
	                from_longitude, 
	                to_latitude, 
	                to_longitude, 
                    (weight->@profile)::real,(direction->@profile)::smallint
                    FROM edge WHERE (weight->@profile)::real>0 AND direction?@profile AND guid=@guid",
                    connection2))
            {
                command.Parameters.Add("id", SqliteType.Integer);
                command.Parameters.Add("fromNode", SqliteType.Integer);
                command.Parameters.Add("toNode", SqliteType.Integer);
                command.Parameters.Add("fromLatitude", SqliteType.Real);
                command.Parameters.Add("fromLongitude", SqliteType.Real);
                command.Parameters.Add("toLatitude", SqliteType.Real);
                command.Parameters.Add("toLongitude", SqliteType.Real);
                command.Parameters.Add("weight", SqliteType.Real);
                command.Parameters.Add("direction", SqliteType.Integer);
                command.Prepare();

                command2.Parameters.AddWithValue("profile", Profile);
                command2.Parameters.AddWithValue("guid", Guid);
                command2.Prepare();

                using var reader2 = await command2.ExecuteReaderAsync();
                while (reader2.Read())
                {
                    command.Parameters["id"].Value = reader2.GetInt64(0);
                    command.Parameters["fromNode"].Value = reader2.GetInt64(1);
                    command.Parameters["toNode"].Value = reader2.GetInt64(2);
                    command.Parameters["fromLatitude"].Value = reader2.GetFloat(3);
                    command.Parameters["fromLongitude"].Value = reader2.GetFloat(4);
                    command.Parameters["toLatitude"].Value = reader2.GetFloat(5);
                    command.Parameters["toLongitude"].Value = reader2.GetFloat(6);
                    command.Parameters["weight"].Value = reader2.GetFloat(7);
                    command.Parameters["direction"].Value = reader2.GetInt16(8);

                    command.ExecuteNonQuery();
                }
            }

            using (var command =
                new SqliteCommand(@"BEGIN TRANSACTION",
                    connection))
            using (var command2 =
                new SqliteCommand(@"INSERT INTO temp_restriction(id) VALUES (@id)",
                    connection))
            using (var command3 =
                new SqliteCommand(@"INSERT INTO temp_restriction_from_edge(rid,edge) VALUES (@id,@edge)",
                    connection))
            using (var command4 =
                new SqliteCommand(@"INSERT INTO temp_restriction_to_edge(rid,edge) VALUES (@id,@edge)",
                    connection))
            using (var command5 =
                new SqliteCommand(@"INSERT INTO temp_restriction_via_node(rid,node) VALUES (@id,@node)",
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

            using (var command =
                new SqliteCommand(@"INSERT OR REPLACE INTO temp_dijkstra (
	                node,
	                weight,
                    weight1,
	                edge,
	                in_queue
                )
                VALUES (
	                @node,
                    6371000*acos(max(-1,min(1,sin(radians(@latitude1))*sin(radians(@latitude))+cos(radians(@latitude1))*cos(radians(@latitude))*cos(radians(@longitude1-@longitude))))),
	                0,
	                0,
	                1
                )", connection))
            {
                command.Parameters.Add("node", SqliteType.Integer);
                command.Parameters.Add("latitude1", SqliteType.Real);
                command.Parameters.Add("longitude1", SqliteType.Real);
                command.Parameters.AddWithValue("latitude", Target.Coordinate.Latitude);
                command.Parameters.AddWithValue("longitude", Target.Coordinate.Longitude);
                command.Prepare();

                if (new[] {0, 1, 3, 4}.Contains(Source.Direction))
                {
                    command.Parameters["node"].Value = Source.ToNode;
                    var coords = Source.Coordinates.Last();
                    command.Parameters["latitude1"].Value = coords.Latitude;
                    command.Parameters["longitude1"].Value = coords.Longitude;
                    await command.ExecuteNonQueryAsync();
                }

                if (new[] {0, 2, 3, 5}.Contains(Source.Direction))
                {
                    command.Parameters["node"].Value = Source.FromNode;
                    var coords = Source.Coordinates.First();
                    command.Parameters["latitude1"].Value = coords.Latitude;
                    command.Parameters["longitude1"].Value = coords.Longitude;
                    await command.ExecuteNonQueryAsync();
                }
            }

            var node = 0L;

            using (var command1 =
                new SqliteCommand(
                    string.Join(";", @"SELECT node FROM temp_dijkstra WHERE in_queue ORDER BY weight LIMIT 1"),
                    connection))
            using (var command2 =
                new SqliteCommand(string.Join(";", @"INSERT OR REPLACE INTO temp_dijkstra (
	                    node,
	                    weight,
	                    weight1,
	                    edge,
	                    in_queue
                    )
                    WITH cte AS
                    (
	                    SELECT *,ROW_NUMBER() OVER (PARTITION BY node ORDER BY weight) AS rn FROM (
		                    SELECT e.to_node AS node,6371000*acos(max(-1,min(1,sin(radians(e.to_latitude))*sin(radians(@latitude))+
                            cos(radians(e.to_latitude))*cos(radians(@latitude))*cos(radians(e.to_longitude-@longitude)))))+
                            t.weight1+e.weight AS weight,t.weight1+e.weight AS weight1,e.id AS edge,1 AS in_queue
		                    FROM temp_edge e JOIN temp_dijkstra t ON e.from_node=t.node
                            WHERE (e.direction=0 OR e.direction=1 OR e.direction=3 OR e.direction=4) AND t.node=@node
                            AND NOT EXISTS (SELECT * FROM  temp_restriction r 
                            JOIN temp_restriction_via_node vn ON vn.node=t.node AND r.id=vn.rid
                            JOIN temp_restriction_to_edge rt ON rt.edge=e.id AND r.id=rt.rid
                            JOIN temp_restriction_from_edge rf ON rf.edge=t.edge AND r.id=rf.rid)
                            UNION ALL SELECT e.from_node AS node,6371000*acos(max(-1,min(1,sin(radians(e.from_latitude))*sin(radians(@latitude))+
                            cos(radians(e.from_latitude))*cos(radians(@latitude))*cos(radians(e.from_longitude-@longitude)))))+
                            t.weight1+e.weight AS weight,t.weight1+e.weight AS weight1,e.id AS edge,1 AS in_queue
		                    FROM temp_edge e JOIN temp_dijkstra t ON e.to_node=t.node
                            WHERE (e.direction=0 OR e.direction=2 OR e.direction=3 OR e.direction=5) AND t.node=@node
                            AND NOT EXISTS (SELECT * FROM  temp_restriction r 
                            JOIN temp_restriction_via_node vn ON vn.node=t.node AND r.id=vn.rid
                            JOIN temp_restriction_to_edge rt ON rt.edge=e.id AND r.id=rt.rid
                            JOIN temp_restriction_from_edge rf ON rf.edge=t.edge AND r.id=rf.rid)
                            UNION ALL SELECT t1.node AS node,t1.weight,t1.weight1,t1.edge,t1.in_queue
		                    FROM temp_edge e JOIN temp_dijkstra t ON e.from_node=t.node JOIN temp_dijkstra t1 ON e.to_node=t1.node
                            WHERE (e.direction=0 OR e.direction=1 OR e.direction=3 OR e.direction=4) AND t.node=@node
                            UNION ALL SELECT t1.node AS node,t1.weight,t1.weight1,t1.edge,t1.in_queue
		                    FROM temp_edge e JOIN temp_dijkstra t ON e.to_node=t.node JOIN temp_dijkstra t1 ON e.from_node=t1.node
                            WHERE (e.direction=0 OR e.direction=2 OR e.direction=3 OR e.direction=5) AND t.node=@node) q
                    )
                    SELECT 
	                    node,
	                    weight,
	                    weight1,
	                    edge,
	                    in_queue
                    FROM cte
                    WHERE rn = 1",
                    @"UPDATE temp_dijkstra SET in_queue=0 WHERE node=@node"), connection))
            {
                command2.Parameters.Add("level", SqliteType.Integer);
                command2.Parameters.AddWithValue("latitude", Target.Coordinate.Latitude);
                command2.Parameters.AddWithValue("longitude", Target.Coordinate.Longitude);
                command1.Prepare();
                command2.Prepare();

                for (var step = 0L;; step++)
                {
                    using (var reader = command1.ExecuteReader())
                    {
                        if (!reader.Read()) break;
                        node = reader.GetInt64(0);
                    }

                    command2.Parameters["node"].Value = node;
                    command2.ExecuteNonQuery();

                    if (step % 1000 == 0) Console.WriteLine($"Step {step} complete");
                }
            }

            var targets = new List<long>();
            if (new[] {0, 1, 3, 4}.Contains(Target.Direction)) targets.Add(Target.FromNode);
            if (new[] {0, 2, 3, 5}.Contains(Target.Direction)) targets.Add(Target.ToNode);

            using (var command =
                new SqliteCommand(
                    @"SELECT node FROM temp_dijkstra WHERE node=@targetFirst OR node=@targetLast ORDER BY weight LIMIT 1",
                    connection))
            {
                command.Parameters.AddWithValue("targetFirst", targets.First());
                command.Parameters.AddWithValue("targetLast", targets.Last());
                command.Prepare();
                using var reader = await command.ExecuteReaderAsync();
                if (!reader.Read()) throw new NullReferenceException();
                node = reader.GetInt64(0);
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

                list.Reverse();
                return list;
            }
        }
    }
}