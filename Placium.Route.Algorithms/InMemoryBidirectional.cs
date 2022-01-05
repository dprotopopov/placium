using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using Npgsql;
using Placium.Route.Common;
using Route.LocalGeo;

namespace Placium.Route.Algorithms
{
    public class InMemoryBidirectional : BasePathFinderAlgorithm
    {
        public InMemoryBidirectional(Guid guid, string connectionString, string vehicleType, string profile,
            float factor) :
            base(guid, connectionString, vehicleType, profile, factor)
        {
        }

        public override async Task<List<long>> FindPathAsync(RouterPoint source,
            RouterPoint target, float maxWeight = float.MaxValue)
        {
            using var connection = new SqliteConnection($"Data source=file:{Guid}?mode=memory&cache=shared");
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
                new SqliteCommand(string.Join(";", "PRAGMA synchronous = OFF",
                    @"CREATE TEMP TABLE temp_dijkstra1 (
	                node INTEGER PRIMARY KEY NOT NULL, 
	                weight REAL NOT NULL, 
	                weight1 REAL NOT NULL, 
	                edge INTEGER NOT NULL,
	                step INTEGER NOT NULL
                )", @"CREATE TEMP TABLE temp_dijkstra2 (
	                node INTEGER PRIMARY KEY NOT NULL, 
	                weight REAL NOT NULL, 
	                weight1 REAL NOT NULL, 
	                edge INTEGER NOT NULL,
	                step INTEGER NOT NULL
                )", @"CREATE TEMP TABLE shared_edge (
	                id INTEGER PRIMARY KEY NOT NULL, 
	                from_node INTEGER NOT NULL, 
	                to_node INTEGER NOT NULL,
	                from_latitude REAL NOT NULL, 
	                from_longitude REAL NOT NULL, 
	                to_latitude REAL NOT NULL, 
	                to_longitude REAL NOT NULL, 
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

            using (var command = new SqliteCommand(
                @"INSERT INTO shared_edge (id,from_node,to_node,
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
                        @"CREATE INDEX temp_dijkstra1_step_idx ON temp_dijkstra1 (step)",
                        @"CREATE INDEX temp_dijkstra1_weight_idx ON temp_dijkstra1 (weight)",
                        @"CREATE INDEX temp_dijkstra2_step_idx ON temp_dijkstra2 (step)",
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
	                step
                )
                VALUES (
	                @node,
                    @factor*distanceInMeters(@latitude,@longitude,@latitude1,@longitude1),
	                0,
	                0,
	                0
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
	                step
                )
                VALUES (
	                @node,
                    @factor*distanceInMeters(@latitude,@longitude,@latitude1,@longitude1),
	                0,
	                0,
	                0
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

            using (var command1 =
                new SqliteCommand(
                    string.Join(";", @"SELECT COUNT(*) FROM temp_dijkstra1 WHERE step=@step",
                        @"SELECT COUNT(*) FROM temp_dijkstra2 WHERE step=@step"),
                    connection))
            using (var command2 =
                new SqliteCommand(string.Join(";", @"INSERT INTO temp_dijkstra1 (
	                    node,
	                    weight,
	                    weight1,
	                    edge,
	                    step
                    )
                    WITH cte AS
                    (
	                    SELECT *,ROW_NUMBER() OVER (PARTITION BY node ORDER BY weight) AS rn FROM (
		                    SELECT e.to_node AS node,@factor*distanceInMeters(e.to_latitude,e.to_longitude,@latitude1,@longitude1)+
                            t.weight1+e.weight AS weight,t.weight1+e.weight AS weight1,e.id AS edge,t.step+1 AS step
		                    FROM shared_edge e JOIN temp_dijkstra1 t ON e.from_node=t.node
                            WHERE (e.direction=0 OR e.direction=1 OR e.direction=3 OR e.direction=4) AND t.step=@step
                            AND NOT EXISTS (SELECT * FROM shared_restriction r 
                            JOIN shared_restriction_via_node vn ON vn.node=t.node AND r.id=vn.rid
                            JOIN shared_restriction_to_edge rt ON rt.edge=e.id AND r.id=rt.rid
                            JOIN shared_restriction_from_edge rf ON rf.edge=t.edge AND r.id=rf.rid)
                            UNION ALL SELECT e.from_node AS node,@factor*distanceInMeters(e.from_latitude,e.from_longitude,@latitude1,@longitude1)+
                            t.weight1+e.weight AS weight,t.weight1+e.weight AS weight1,e.id AS edge,t.step+1 AS step
		                    FROM shared_edge e JOIN temp_dijkstra1 t ON e.to_node=t.node
                            WHERE (e.direction=0 OR e.direction=2 OR e.direction=3 OR e.direction=5) AND t.step=@step
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
	                    step
                    FROM cte
                    WHERE rn = 1
                    ON CONFLICT (node) DO UPDATE SET
	                    weight=EXCLUDED.weight,
	                    weight1=EXCLUDED.weight1,
	                    edge=EXCLUDED.edge,
                        step=EXCLUDED.step
                        WHERE temp_dijkstra1.weight>EXCLUDED.weight", @"INSERT INTO temp_dijkstra2 (
	                    node,
	                    weight,
	                    weight1,
	                    edge,
	                    step
                    )
                    WITH cte AS
                    (
	                    SELECT *,ROW_NUMBER() OVER (PARTITION BY node ORDER BY weight) AS rn FROM (
		                    SELECT e.from_node AS node,@factor*distanceInMeters(e.from_latitude,e.from_longitude,@latitude2,@longitude2)+
                            t.weight1+e.weight AS weight,t.weight1+e.weight AS weight1,e.id AS edge,t.step+1 AS step
		                    FROM shared_edge e JOIN temp_dijkstra2 t ON e.to_node=t.node
                            WHERE (e.direction=0 OR e.direction=1 OR e.direction=3 OR e.direction=4) AND t.step=@step
                            AND NOT EXISTS (SELECT * FROM shared_restriction r 
                            JOIN shared_restriction_via_node vn ON vn.node=t.node AND r.id=vn.rid
                            JOIN shared_restriction_to_edge rt ON rt.edge=e.id AND r.id=rt.rid
                            JOIN shared_restriction_from_edge rf ON rf.edge=t.edge AND r.id=rf.rid)
                            UNION ALL SELECT e.to_node AS node,@factor*distanceInMeters(e.to_latitude,e.to_longitude,@latitude2,@longitude2)+
                            t.weight1+e.weight AS weight,t.weight1+e.weight AS weight1,e.id AS edge,t.step+1 AS step
		                    FROM shared_edge e JOIN temp_dijkstra2 t ON e.from_node=t.node
                            WHERE (e.direction=0 OR e.direction=2 OR e.direction=3 OR e.direction=5) AND t.step=@step
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
	                    step
                    FROM cte
                    WHERE rn = 1
                    ON CONFLICT (node) DO UPDATE SET
	                    weight=EXCLUDED.weight,
	                    weight1=EXCLUDED.weight1,
	                    edge=EXCLUDED.edge,
                        step=EXCLUDED.step
                        WHERE temp_dijkstra2.weight>EXCLUDED.weight"), connection))
            using (var command3 =
                new SqliteCommand(string.Join(";", @"SELECT t1.node,t1.weight1+t2.weight1 FROM temp_dijkstra1 t1
                JOIN temp_dijkstra2 t2 ON t1.node=t2.node WHERE NOT EXISTS (SELECT * FROM shared_restriction r 
                JOIN shared_restriction_via_node vn ON vn.node=t1.node AND r.id=vn.rid
                JOIN shared_restriction_to_edge rt ON rt.edge=t2.edge AND r.id=rt.rid
                JOIN shared_restriction_from_edge rf ON rf.edge=t1.edge AND r.id=rf.rid)
                ORDER BY t1.weight1+t2.weight1 LIMIT 1"),
                    connection))
            using (var command4 =
                new SqliteCommand(string.Join(";",
                        @"DELETE FROM temp_dijkstra1 WHERE weight>@maxWeight",
                        @"DELETE FROM temp_dijkstra2 WHERE weight>@maxWeight"),
                    connection))
            {
                command1.Parameters.Add("step", SqliteType.Integer);
                command2.Parameters.Add("step", SqliteType.Integer);
                command2.Parameters.AddWithValue("latitude1", target.Coordinate.Latitude);
                command2.Parameters.AddWithValue("longitude1", target.Coordinate.Longitude);
                command2.Parameters.AddWithValue("latitude2", source.Coordinate.Latitude);
                command2.Parameters.AddWithValue("longitude2", source.Coordinate.Longitude);
                command2.Parameters.AddWithValue("factor", Factor);
                command4.Parameters.Add("maxWeight", SqliteType.Real);
                command1.Prepare();
                command2.Prepare();
                command3.Prepare();
                command4.Prepare();

                for (var step = 0L;; step++)
                {
                    command1.Parameters["step"].Value = step;
                    var count1 = 0L;
                    var count2 = 0L;

                    using (var reader = command1.ExecuteReader())
                    {
                        reader.Read();
                        count1 = reader.GetInt64(0);
                        reader.NextResult();
                        reader.Read();
                        count2 = reader.GetInt64(0);
                    }

                    if (count1 > 0 || count2 > 0)
                    {
                        command2.Parameters["step"].Value = step;
                        command2.ExecuteNonQuery();
                    }
                    else
                    {
                        break;
                    }

                    using (var reader = command3.ExecuteReader())
                    {
                        if (reader.Read())
                        {
                            node = reader.GetInt64(0);
                            maxWeight = reader.GetFloat(1);
                        }
                    }

                    command4.Parameters["maxWeight"].Value = maxWeight;
                    command4.ExecuteNonQuery();

                    if (step % 1 == 0) Console.WriteLine($"Step {step} complete count1={count1} count2={count2} maxWeight={maxWeight}");
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

            return list;
        }
    }
}