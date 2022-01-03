using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using Placium.Common;
using Placium.Route.Common;

namespace Placium.Route.Algorithms
{
    public class Bidirectional : BasePathFinderAlgorithm
    {
        public Bidirectional(Guid guid, string connectionString, string vehicleType, string profile, float factor) :
            base(guid,
                connectionString, vehicleType, profile, factor)
        {
        }


        public override async Task<List<long>> FindPathAsync(RouterPoint source,
            RouterPoint target)
        {
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
                $$", @"CREATE TEMP TABLE temp_dijkstra1 (
	                node BIGINT PRIMARY KEY NOT NULL, 
	                weight REAL NOT NULL, 
	                weight1 REAL NOT NULL, 
	                edge BIGINT NOT NULL,
	                step INTEGER NOT NULL
                )", @"CREATE TEMP TABLE temp_dijkstra2 (
	                node BIGINT PRIMARY KEY NOT NULL, 
	                weight REAL NOT NULL, 
	                weight1 REAL NOT NULL, 
	                edge BIGINT NOT NULL,
	                step INTEGER NOT NULL
                )", @"CREATE TEMP TABLE shared_edge (
	                id BIGINT PRIMARY KEY NOT NULL, 
	                from_node BIGINT NOT NULL, 
	                to_node BIGINT NOT NULL,
	                from_latitude REAL NOT NULL, 
	                from_longitude REAL NOT NULL, 
	                to_latitude REAL NOT NULL, 
	                to_longitude REAL NOT NULL, 
	                weight REAL NOT NULL,
                    direction SMALLINT NOT NULL
                )", @"CREATE TEMP TABLE shared_restriction (
	                id BIGINT PRIMARY KEY NOT NULL
                )", @"CREATE TEMP TABLE shared_restriction_from_edge (
	                id BIGSERIAL PRIMARY KEY NOT NULL, 
	                rid BIGINT NOT NULL REFERENCES shared_restriction (id), 
	                edge BIGINT NOT NULL
                )", @"CREATE TEMP TABLE shared_restriction_to_edge (
	                id BIGSERIAL PRIMARY KEY NOT NULL, 
	                rid BIGINT NOT NULL REFERENCES shared_restriction (id), 
	                edge BIGINT NOT NULL
                )", @"CREATE TEMP TABLE shared_restriction_via_node (
	                id BIGSERIAL PRIMARY KEY NOT NULL, 
	                rid BIGINT NOT NULL REFERENCES shared_restriction (id), 
	                node BIGINT NOT NULL
                )"), connection))
            {
                command.Prepare();
                await command.ExecuteNonQueryAsync();
            }

            using (var writer = connection.BeginTextImport(
                @"COPY shared_edge (id,from_node,to_node,
                    from_latitude, 
                    from_longitude,
                    to_latitude,
                    to_longitude,
                    weight, direction) FROM STDIN WITH NULL AS ''")
            )
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
                command2.Parameters.AddWithValue("profile", Profile);
                command2.Parameters.AddWithValue("guid", Guid);
                command2.Prepare();

                using var reader2 = await command2.ExecuteReaderAsync();
                while (reader2.Read())
                {
                    var values = new[]
                    {
                        reader2.GetInt64(0).ToString(),
                        reader2.GetInt64(1).ToString(),
                        reader2.GetInt64(2).ToString(),
                        reader2.GetFloat(3).ValueAsText(),
                        reader2.GetFloat(4).ValueAsText(),
                        reader2.GetFloat(5).ValueAsText(),
                        reader2.GetFloat(6).ValueAsText(),
                        reader2.GetFloat(7).ValueAsText(),
                        reader2.GetInt16(8).ToString()
                    };

                    writer.WriteLine(string.Join("\t", values));
                }
            }


            using (var command =
                new NpgsqlCommand(@"BEGIN", connection))
            using (var command6 =
                new NpgsqlCommand(@"COMMIT", connection))
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

                await command.ExecuteNonQueryAsync();

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

                await command6.ExecuteNonQueryAsync();
            }

            using (var command =
                new NpgsqlCommand(string.Join(";",
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
                new NpgsqlCommand(@"INSERT INTO temp_dijkstra1 (
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
                )
                ON CONFLICT (node) DO NOTHING", connection))
            {
                command.Parameters.Add("node", NpgsqlDbType.Bigint);
                command.Parameters.Add("latitude1", NpgsqlDbType.Real);
                command.Parameters.Add("longitude1", NpgsqlDbType.Real);
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
                new NpgsqlCommand(@"INSERT INTO temp_dijkstra2 (
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
                )
                ON CONFLICT (node) DO NOTHING", connection))
            {
                command.Parameters.Add("node", NpgsqlDbType.Bigint);
                command.Parameters.Add("latitude1", NpgsqlDbType.Real);
                command.Parameters.Add("longitude1", NpgsqlDbType.Real);
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
            var weight = 0f;

            using (var command1 =
                new NpgsqlCommand(
                    string.Join(";", @"SELECT COUNT(*) FROM temp_dijkstra1 WHERE step=@step",
                        @"SELECT COUNT(*) FROM temp_dijkstra2 WHERE step=@step"),
                    connection))
            using (var command2 =
                new NpgsqlCommand(string.Join(";", @"INSERT INTO temp_dijkstra1 (
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
                            WHERE e.direction=ANY(ARRAY[0,1,3,4]) AND t.step=@step 
                            AND NOT EXISTS (SELECT * FROM  shared_restriction r 
                            JOIN shared_restriction_via_node vn ON vn.node=t.node AND r.id=vn.rid
                            JOIN shared_restriction_to_edge rt ON rt.edge=e.id AND r.id=rt.rid
                            JOIN shared_restriction_from_edge rf ON rf.edge=t.edge AND r.id=rf.rid)
                            UNION ALL SELECT e.from_node AS node,@factor*distanceInMeters(e.from_latitude,e.from_longitude,@latitude1,@longitude1)+
                            t.weight1+e.weight AS weight,t.weight1+e.weight AS weight1,e.id AS edge,t.step+1 AS step
		                    FROM shared_edge e JOIN temp_dijkstra1 t ON e.to_node=t.node
                            WHERE e.direction=ANY(ARRAY[0,2,3,5]) AND t.step=@step
                            AND NOT EXISTS (SELECT * FROM  shared_restriction r 
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
                            WHERE e.direction=ANY(ARRAY[0,1,3,4]) AND t.step=@step 
                            AND NOT EXISTS (SELECT * FROM  shared_restriction r 
                            JOIN shared_restriction_via_node vn ON vn.node=t.node AND r.id=vn.rid
                            JOIN shared_restriction_to_edge rt ON rt.edge=e.id AND r.id=rt.rid
                            JOIN shared_restriction_from_edge rf ON rf.edge=t.edge AND r.id=rf.rid)
                            UNION ALL SELECT e.to_node AS node,@factor*distanceInMeters(e.to_latitude,e.to_longitude,@latitude2,@longitude2)+
                            t.weight1+e.weight AS weight,t.weight1+e.weight AS weight1,e.id AS edge,t.step+1 AS step
		                    FROM shared_edge e JOIN temp_dijkstra2 t ON e.from_node=t.node
                            WHERE e.direction=ANY(ARRAY[0,2,3,5]) AND t.step=@step
                            AND NOT EXISTS (SELECT * FROM  shared_restriction r 
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
                new NpgsqlCommand(string.Join(";", @"SELECT t1.node,t1.weight1+t2.weight1 FROM temp_dijkstra1 t1
                JOIN temp_dijkstra2 t2 ON t1.node=t2.node WHERE NOT EXISTS (SELECT * FROM  shared_restriction r 
                JOIN shared_restriction_via_node vn ON vn.node=t1.node AND r.id=vn.rid
                JOIN shared_restriction_to_edge rt ON rt.edge=t2.edge AND r.id=rt.rid
                JOIN shared_restriction_from_edge rf ON rf.edge=t1.edge AND r.id=rf.rid)
                ORDER BY t1.weight1+t2.weight1 LIMIT 1"),
                    connection))
            using (var command4 =
                new NpgsqlCommand(string.Join(";",
                        @"DELETE FROM temp_dijkstra1 WHERE weight>=@weight",
                        @"DELETE FROM temp_dijkstra2 WHERE weight>=@weight"),
                    connection))
            {
                command1.Parameters.Add("step", NpgsqlDbType.Integer);
                command2.Parameters.Add("step", NpgsqlDbType.Integer);
                command2.Parameters.AddWithValue("latitude1", target.Coordinate.Latitude);
                command2.Parameters.AddWithValue("longitude1", target.Coordinate.Longitude);
                command2.Parameters.AddWithValue("latitude2", source.Coordinate.Latitude);
                command2.Parameters.AddWithValue("longitude2", source.Coordinate.Longitude);
                command2.Parameters.AddWithValue("factor", Factor);
                command4.Parameters.Add("weight", NpgsqlDbType.Real);
                command1.Prepare();
                command2.Prepare();
                command3.Prepare();
                command4.Prepare();

                for (var step = 0;; step++)
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
                            weight = reader.GetFloat(1);
                        }
                    }

                    if (node != 0)
                    {
                        command4.Parameters["weight"].Value = weight;
                        command4.ExecuteNonQuery();
                    }

                    if (step % 1 == 0) Console.WriteLine($"Step {step} complete count1={count1} count2={count2}");
                }
            }

            var list = new List<long>();
            using (var command =
                new NpgsqlCommand(@"SELECT e.from_node,e.id 
                    FROM temp_dijkstra1 t JOIN shared_edge e ON t.edge=e.id
                    WHERE t.node=@node AND e.to_node=t.node
                    UNION ALL SELECT e.to_node,e.id 
                    FROM temp_dijkstra1 t JOIN shared_edge e ON t.edge=e.id
                    WHERE t.node=@node AND e.from_node=t.node", connection))
            {
                command.Parameters.Add("node", NpgsqlDbType.Bigint);
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
                new NpgsqlCommand(@"SELECT e.from_node,e.id 
                    FROM temp_dijkstra2 t JOIN shared_edge e ON t.edge=e.id
                    WHERE t.node=@node AND e.to_node=t.node
                    UNION ALL SELECT e.to_node,e.id 
                    FROM temp_dijkstra2 t JOIN shared_edge e ON t.edge=e.id
                    WHERE t.node=@node AND e.from_node=t.node", connection))
            {
                command.Parameters.Add("node", NpgsqlDbType.Bigint);
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