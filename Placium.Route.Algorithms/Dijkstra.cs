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
    public class Dijkstra : BaseDatabaseAlgorithm<List<long>>
    {
        public Dijkstra(Guid guid, string connectionString, string profile, RouterPoint source,
            RouterPoint target) : base(guid, connectionString, profile)
        {
            Source = source;
            Target = target;
        }

        public RouterPoint Source { get; }
        public RouterPoint Target { get; }

        public override async Task<List<long>> DoRunAsync()
        {
            using var connection = new NpgsqlConnection(ConnectionString);
            using var connection2 = new NpgsqlConnection(ConnectionString);
            await connection.OpenAsync();
            await connection2.OpenAsync();

            var except = new[]
            {
                Source.EdgeId,
                Target.EdgeId
            };

            using (var command =
                new NpgsqlCommand(string.Join(";", @"CREATE TEMP TABLE temp_dijkstra (
	                node BIGINT NOT NULL, 
	                weight REAL, 
	                edge BIGINT,
	                in_queue BOOLEAN,
	                PRIMARY KEY (node)
                )", @"CREATE TEMP TABLE temp_edge (
	                id BIGINT NOT NULL, 
	                from_node BIGINT, 
	                to_node BIGINT,
	                weight REAL,
                    direction SMALLINT,
	                PRIMARY KEY (id)
                )"), connection))
            {
                command.Prepare();
                await command.ExecuteNonQueryAsync();
            }

            using (var writer = connection.BeginTextImport(
                "COPY temp_edge (id,from_node,to_node,weight,direction) FROM STDIN WITH NULL AS ''")
            )
            using (var command2 =
                new NpgsqlCommand(
                    @"SELECT id,from_node,to_node,(weight->@profile)::real,(direction->@profile)::smallint
                    FROM edge WHERE (weight->@profile)::real>0 AND direction?@profile AND guid=@guid AND id!=ANY(@except)",
                    connection2))
            {
                command2.Parameters.AddWithValue("except", except);
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
                        reader2.GetInt16(4).ToString()
                    };

                    writer.WriteLine(string.Join("\t", values));
                }
            }

            using (var command =
                new NpgsqlCommand(string.Join(";",
                        @"CREATE INDEX temp_dijkstra_in_queue_idx ON temp_dijkstra (in_queue)",
                        @"CREATE INDEX temp_dijkstra_weight_idx ON temp_dijkstra (weight)",
                        @"CREATE UNIQUE INDEX temp_edge_from_node_to_node_idx ON temp_edge (from_node,to_node)"),
                    connection))
            {
                command.Prepare();
                await command.ExecuteNonQueryAsync();
            }

            using (var command =
                new NpgsqlCommand(@"INSERT INTO temp_dijkstra (
	                node,
	                weight,
	                edge,
	                in_queue
                )
                VALUES (
	                @node,
	                0,
	                0,
	                true
                )
                ON CONFLICT (node) DO NOTHING", connection))
            {
                command.Parameters.Add("node", NpgsqlDbType.Bigint);
                command.Prepare();

                if (new[] {0, 1, 3, 4}.Contains(Source.Direction))
                {
                    command.Parameters["node"].Value = Source.ToNode;
                    await command.ExecuteNonQueryAsync();
                }

                if (new[] {0, 2, 3, 5}.Contains(Source.Direction))
                {
                    command.Parameters["node"].Value = Source.FromNode;
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
	                    edge,
	                    in_queue
                    )
                    WITH cte AS
                    (
	                    SELECT *,ROW_NUMBER() OVER (PARTITION BY node ORDER BY weight) AS rn FROM (
		                    SELECT e.to_node AS node,t.weight+e.weight AS weight,e.id AS edge,true AS in_queue
		                    FROM temp_edge e JOIN temp_dijkstra t ON e.from_node=t.node
                            WHERE e.direction=ANY(ARRAY[0,1,3,4]) AND t.node=@node
                            UNION ALL SELECT e.from_node AS node,t.weight+e.weight AS weight,e.id AS edge,true AS in_queue
		                    FROM temp_edge e JOIN temp_dijkstra t ON e.to_node=t.node
                            WHERE e.direction=ANY(ARRAY[0,2,3,5]) AND t.node=@node) q
                    )
                    SELECT 
	                    node,
	                    weight,
	                    edge,
	                    in_queue
                    FROM cte
                    WHERE rn = 1
                    ON CONFLICT (node) DO UPDATE SET
	                    weight=EXCLUDED.weight,
	                    edge=EXCLUDED.edge,
                        in_queue=EXCLUDED.in_queue
                        WHERE temp_dijkstra.weight>EXCLUDED.weight",
                    @"UPDATE temp_dijkstra SET in_queue=false WHERE node=@node"), connection))
            {
                command2.Parameters.Add("node", NpgsqlDbType.Bigint);
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