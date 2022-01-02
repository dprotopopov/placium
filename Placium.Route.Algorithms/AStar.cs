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
    public class AStar : BaseDatabaseAlgorithm<List<long>>
    {
        public AStar(Guid guid, string connectionString, string vehicleType, string profile, RouterPoint source,
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
            using var connection = new NpgsqlConnection(ConnectionString);
            using var connection2 = new NpgsqlConnection(ConnectionString);
            await connection.OpenAsync();
            await connection2.OpenAsync();

            using (var command =
                new NpgsqlCommand(string.Join(";", @"CREATE TEMP TABLE temp_dijkstra (
	                node BIGINT PRIMARY KEY NOT NULL, 
	                weight REAL NOT NULL, 
	                weight1 REAL NOT NULL, 
	                edge BIGINT NOT NULL,
	                in_queue BOOLEAN NOT NULL
                )", @"CREATE TEMP TABLE temp_edge (
	                id BIGINT PRIMARY KEY NOT NULL, 
	                from_node BIGINT NOT NULL, 
	                to_node BIGINT NOT NULL,
	                from_latitude REAL NOT NULL, 
	                from_longitude REAL NOT NULL, 
	                to_latitude REAL NOT NULL, 
	                to_longitude REAL NOT NULL, 
	                weight REAL NOT NULL,
                    direction SMALLINT NOT NULL
                )", @"CREATE TEMP TABLE temp_restriction (
	                id BIGINT PRIMARY KEY NOT NULL
                )", @"CREATE TEMP TABLE temp_restriction_from_edge (
	                id BIGSERIAL PRIMARY KEY NOT NULL, 
	                rid BIGINT NOT NULL REFERENCES temp_restriction (id), 
	                edge BIGINT NOT NULL
                )", @"CREATE TEMP TABLE temp_restriction_to_edge (
	                id BIGSERIAL PRIMARY KEY NOT NULL, 
	                rid BIGINT NOT NULL REFERENCES temp_restriction (id), 
	                edge BIGINT NOT NULL
                )", @"CREATE TEMP TABLE temp_restriction_via_node (
	                id BIGSERIAL PRIMARY KEY NOT NULL, 
	                rid BIGINT NOT NULL REFERENCES temp_restriction (id), 
	                node BIGINT NOT NULL
                )"), connection))
            {
                command.Prepare();
                await command.ExecuteNonQueryAsync();
            }

            using (var writer = connection.BeginTextImport(
                @"COPY temp_edge (id,from_node,to_node,
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
            using (var command2 =
                new NpgsqlCommand(@"INSERT INTO temp_restriction(id) VALUES (@id)",
                    connection))
            using (var command3 =
                new NpgsqlCommand(@"INSERT INTO temp_restriction_from_edge(rid,edge) VALUES (@id,@edge)",
                    connection))
            using (var command4 =
                new NpgsqlCommand(@"INSERT INTO temp_restriction_to_edge(rid,edge) VALUES (@id,@edge)",
                    connection))
            using (var command5 =
                new NpgsqlCommand(@"INSERT INTO temp_restriction_via_node(rid,node) VALUES (@id,@node)",
                    connection))
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
                command2.Parameters.Add("id", NpgsqlDbType.Bigint);
                command2.Prepare();

                command3.Parameters.Add("id", NpgsqlDbType.Bigint);
                command3.Parameters.Add("edge", NpgsqlDbType.Bigint);
                command3.Prepare();

                command4.Parameters.Add("id", NpgsqlDbType.Bigint);
                command4.Parameters.Add("edge", NpgsqlDbType.Bigint);
                command4.Prepare();

                command5.Parameters.Add("id", NpgsqlDbType.Bigint);
                command5.Parameters.Add("node", NpgsqlDbType.Bigint);
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
                new NpgsqlCommand(string.Join(";",
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
                new NpgsqlCommand(@"INSERT INTO temp_dijkstra (
	                node,
	                weight,
                    weight1,
	                edge,
	                in_queue
                )
                VALUES (
	                @node,
                    6371000*acos(max(-1,min(1,sin(radians(@latitude1))*sin(radians(@latitude))+cos(radians(@latitude1))*cos(radians(@latitude))*cos(radians(@longitude1-@longitude)))))::real,
	                0,
	                0,
	                true
                )
                ON CONFLICT (node) DO NOTHING", connection))
            {
                command.Parameters.Add("node", NpgsqlDbType.Bigint);
                command.Parameters.Add("latitude1", NpgsqlDbType.Real);
                command.Parameters.Add("longitude1", NpgsqlDbType.Real);
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
		                    SELECT e.to_node AS node,6371000*acos(max(-1,min(1,sin(radians(e.to_latitude))*sin(radians(@latitude))+
                            cos(radians(e.to_latitude))*cos(radians(@latitude))*cos(radians(e.to_longitude-@longitude)))))::real+
                            t.weight1+e.weight AS weight,t.weight1+e.weight AS weight1,e.id AS edge,true AS in_queue
		                    FROM temp_edge e JOIN temp_dijkstra t ON e.from_node=t.node
                            WHERE e.direction=ANY(ARRAY[0,1,3,4]) AND t.node=@node 
                            AND NOT EXISTS (SELECT * FROM  temp_restriction r 
                            JOIN temp_restriction_via_node vn ON vn.node=t.node AND r.id=vn.rid
                            JOIN temp_restriction_to_edge rt ON rt.edge=e.id AND r.id=rt.rid
                            JOIN temp_restriction_from_edge rf ON rf.edge=t.edge AND r.id=rf.rid)
                            UNION ALL SELECT e.from_node AS node,6371000*acos(max(-1,min(1,sin(radians(e.from_latitude))*sin(radians(@latitude))+
                            cos(radians(e.from_latitude))*cos(radians(@latitude))*cos(radians(e.from_longitude-@longitude)))))::real+
                            t.weight1+e.weight AS weight,t.weight1+e.weight AS weight1,e.id AS edge,true AS in_queue
		                    FROM temp_edge e JOIN temp_dijkstra t ON e.to_node=t.node
                            WHERE e.direction=ANY(ARRAY[0,2,3,5]) AND t.node=@node
                            AND NOT EXISTS (SELECT * FROM  temp_restriction r 
                            JOIN temp_restriction_via_node vn ON vn.node=t.node AND r.id=vn.rid
                            JOIN temp_restriction_to_edge rt ON rt.edge=e.id AND r.id=rt.rid
                            JOIN temp_restriction_from_edge rf ON rf.edge=t.edge AND r.id=rf.rid)) q
                    )
                    SELECT 
	                    node,
	                    weight,
	                    weight1,
	                    edge,
	                    in_queue
                    FROM cte
                    WHERE rn = 1
                    ON CONFLICT (node) DO UPDATE SET
	                    weight=EXCLUDED.weight,
	                    weight1=EXCLUDED.weight1,
	                    edge=EXCLUDED.edge,
                        in_queue=EXCLUDED.in_queue
                        WHERE temp_dijkstra.weight>EXCLUDED.weight",
                    @"UPDATE temp_dijkstra SET in_queue=false WHERE node=@node"), connection))
            {
                command2.Parameters.Add("node", NpgsqlDbType.Bigint);
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
                new NpgsqlCommand(
                    @"SELECT node FROM temp_dijkstra WHERE node=ANY(@targets) ORDER BY weight LIMIT 1",
                    connection))
            {
                command.Parameters.AddWithValue("targets", targets.ToArray());
                command.Prepare();
                using var reader = await command.ExecuteReaderAsync();
                if (!reader.Read()) throw new NullReferenceException();
                node = reader.GetInt64(0);
            }

            using (var command =
                new NpgsqlCommand(@"SELECT e.from_node,e.id 
                    FROM temp_dijkstra t JOIN temp_edge e ON t.edge=e.id
                    WHERE t.node=@node AND e.to_node=t.node
                    UNION ALL SELECT e.to_node,e.id 
                    FROM temp_dijkstra t JOIN temp_edge e ON t.edge=e.id
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

                list.Reverse();
                return list;
            }
        }
    }
}