using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using Placium.Route.Common;

namespace Placium.Route.Algorithms
{
    public class AStar : BasePathFinderAlgorithm
    {
        public AStar(Guid guid, string connectionString, string vehicleType, string profile, float minFactor,
            float maxFactor) : base(guid,
            connectionString, vehicleType, profile, minFactor, maxFactor)
        {
        }


        public override async Task<PathFinderResult> FindPathAsync(RouterPoint source,
            RouterPoint target, float maxWeight = float.MaxValue)
        {
            var minWeight = MinFactor * 1;

            using var connection = new NpgsqlConnection(ConnectionString);
            await connection.OpenAsync();

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
                $$", @"CREATE TEMP TABLE temp_node (
	                id BIGINT PRIMARY KEY NOT NULL, 
	                from_weight REAL NOT NULL
                )", @"CREATE TEMP TABLE temp_dijkstra (
	                node BIGINT PRIMARY KEY NOT NULL, 
	                weight REAL NOT NULL, 
	                g REAL NOT NULL, 
	                edge BIGINT NOT NULL,
	                in_queue BOOLEAN NOT NULL
                )", @"CREATE TEMP TABLE temp_edge (
	                id BIGINT PRIMARY KEY NOT NULL, 
	                from_node BIGINT NOT NULL, 
	                to_node BIGINT NOT NULL, 
	                weight REAL NOT NULL,
                    direction SMALLINT NOT NULL
                )", @"CREATE TEMP TABLE temp_restriction (
	                id BIGINT PRIMARY KEY NOT NULL
                )", @"CREATE TEMP TABLE temp_restriction_from_edge (
	                rid BIGINT NOT NULL REFERENCES temp_restriction (id), 
	                edge BIGINT NOT NULL
                )", @"CREATE TEMP TABLE temp_restriction_to_edge (
	                rid BIGINT NOT NULL REFERENCES temp_restriction (id), 
	                edge BIGINT NOT NULL
                )", @"CREATE TEMP TABLE temp_restriction_via_node (
	                rid BIGINT NOT NULL REFERENCES temp_restriction (id), 
	                node BIGINT NOT NULL
                )"), connection))
            {
                command.Prepare();
                await command.ExecuteNonQueryAsync();
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

            using var commandBegin =
                new NpgsqlCommand(@"BEGIN", connection);
            using var commandCommit =
                new NpgsqlCommand(@"COMMIT", connection);

            commandBegin.Prepare();
            commandCommit.Prepare();

            using var commandSelectFromRestriction =
                new NpgsqlCommand(string.Join(";", @"INSERT INTO temp_restriction(id) SELECT rid FROM (
                    SELECT rid FROM restriction_via_node WHERE node=@node AND vehicle_type=@vehicleType AND guid=@guid
                    UNION SELECT rid FROM restriction_from_edge r JOIN edge e ON r.edge=e.id
                    WHERE (e.from_node=@node OR e.to_node=@node) AND r.vehicle_type=@vehicleType AND r.guid=@guid AND e.guid=@guid
                    UNION SELECT rid FROM restriction_to_edge r JOIN edge e ON r.edge=e.id
                    WHERE (e.from_node=@node OR e.to_node=@node) AND r.vehicle_type=@vehicleType AND r.guid=@guid AND e.guid=@guid) q
                    GROUP BY rid ON CONFLICT DO NOTHING",
                        @"INSERT INTO temp_restriction_from_edge(rid,edge) SELECT r.rid,r.edge FROM restriction_from_edge r JOIN edge e ON r.edge=e.id
                    WHERE (e.from_node=@node OR e.to_node=@node) AND r.vehicle_type=@vehicleType AND r.guid=@guid AND e.guid=@guid
                    GROUP BY r.rid,r.edge ON CONFLICT DO NOTHING",
                        @"INSERT INTO temp_restriction_to_edge(rid,edge) SELECT DISTINCT r.rid,r.edge FROM restriction_to_edge r JOIN edge e ON r.edge=e.id
                    WHERE (e.from_node=@node OR e.to_node=@node) AND r.vehicle_type=@vehicleType AND r.guid=@guid AND e.guid=@guid
                    GROUP BY r.rid,r.edge ON CONFLICT DO NOTHING",
                        @"INSERT INTO temp_restriction_via_node(rid,node) SELECT rid,node FROM restriction_via_node WHERE node=@node AND vehicle_type=@vehicleType AND guid=@guid ON CONFLICT DO NOTHING"),
                    connection);

            commandSelectFromRestriction.Parameters.Add("node", NpgsqlDbType.Bigint);
            commandSelectFromRestriction.Parameters.AddWithValue("vehicleType", VehicleType);
            commandSelectFromRestriction.Parameters.AddWithValue("guid", Guid);
            commandSelectFromRestriction.Prepare();

            using var commandSelectFromNode =
                new NpgsqlCommand(string.Join(";",
                        @"INSERT INTO temp_node (id,from_weight) SELECT n.id,
                    @factor*distanceInMeters(latitude,longitude,@fromLatitude,@fromLongitude)
                    FROM node n JOIN edge e ON n.id=e.to_node WHERE n.guid=@guid AND e.guid=@guid
                    AND @factor*distanceInMeters(latitude,longitude,@fromLatitude,@fromLongitude)<=@maxWeight
                    AND @node=ANY(e.nodes) UNION ALL SELECT n.id,
                    @factor*distanceInMeters(latitude,longitude,@fromLatitude,@fromLongitude)
                    FROM node n JOIN edge e ON n.id=e.from_node WHERE n.guid=@guid AND e.guid=@guid
                    AND @factor*distanceInMeters(latitude,longitude,@fromLatitude,@fromLongitude)<=@maxWeight
                    AND @node=ANY(e.nodes) ON CONFLICT (id) DO NOTHING", @"INSERT INTO temp_edge (id,from_node,to_node,
                    weight,direction) SELECT id,from_node,to_node,
                    GREATEST((weight->@profile)::real,@minWeight),(direction->@profile)::smallint
                    FROM edge WHERE weight?@profile AND direction?@profile AND guid=@guid
                    AND @node=ANY(nodes) ON CONFLICT (from_node,to_node) DO NOTHING"),
                    connection);

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
                commandSelectFromNode.ExecuteNonQuery();
                commandSelectFromRestriction.ExecuteNonQuery();
                commandCommit.ExecuteNonQuery();
            }

            using (var command =
                new NpgsqlCommand(@"INSERT INTO temp_dijkstra (
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
	                true
                )
                ON CONFLICT (node) DO NOTHING", connection))
            {
                command.Parameters.Add("node", NpgsqlDbType.Bigint);
                command.Parameters.Add("latitude1", NpgsqlDbType.Real);
                command.Parameters.Add("longitude1", NpgsqlDbType.Real);
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
                new NpgsqlCommand(
                    string.Join(";",
                        @"SELECT node,weight,NOT in_queue FROM temp_dijkstra WHERE node=ANY(@sources) ORDER BY weight LIMIT 1",
                        @"SELECT node FROM temp_dijkstra WHERE in_queue ORDER BY weight LIMIT 1"),
                    connection))
            using (var command2 =
                new NpgsqlCommand(string.Join(";", @"INSERT INTO temp_dijkstra (
	                    node,
	                    weight,
	                    g,
	                    edge,
	                    in_queue
                    )
                    WITH cte AS
                    (
	                    SELECT *,ROW_NUMBER() OVER (PARTITION BY node ORDER BY weight) AS rn FROM (
		                    SELECT e.from_node AS node,n.from_weight+t.g+e.weight AS weight,t.g+e.weight AS g,e.id AS edge,true AS in_queue
		                    FROM temp_edge e JOIN temp_node n ON e.from_node=n.id JOIN temp_dijkstra t ON e.to_node=t.node
                            WHERE e.direction=ANY(ARRAY[0,1,3,4]) AND t.node=@node 
                            AND NOT EXISTS (SELECT * FROM temp_restriction r 
                            JOIN temp_restriction_via_node vn ON vn.node=t.node AND r.id=vn.rid
                            JOIN temp_restriction_to_edge rt ON rt.edge=e.id AND r.id=rt.rid
                            JOIN temp_restriction_from_edge rf ON rf.edge=t.edge AND r.id=rf.rid)
                            UNION ALL SELECT e.to_node AS node,n.from_weight+t.g+e.weight AS weight,t.g+e.weight AS g,e.id AS edge,true AS in_queue
		                    FROM temp_edge e JOIN temp_node n ON e.to_node=n.id JOIN temp_dijkstra t ON e.from_node=t.node
                            WHERE e.direction=ANY(ARRAY[0,2,3,5]) AND t.node=@node
                            AND NOT EXISTS (SELECT * FROM temp_restriction r 
                            JOIN temp_restriction_via_node vn ON vn.node=t.node AND r.id=vn.rid
                            JOIN temp_restriction_to_edge rt ON rt.edge=e.id AND r.id=rt.rid
                            JOIN temp_restriction_from_edge rf ON rf.edge=t.edge AND r.id=rf.rid)) q
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
                    @"UPDATE temp_dijkstra SET in_queue=false WHERE node=@node",
                    @"DELETE FROM temp_dijkstra WHERE weight>@maxWeight"), connection))
            {
                command.Parameters.AddWithValue("sources", sources.ToArray());
                command2.Parameters.Add("maxWeight", NpgsqlDbType.Real);
                command2.Parameters.Add("node", NpgsqlDbType.Bigint);
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


                    if (step % 100 == 0) Console.WriteLine($"{DateTime.Now:O} Step {step} complete");
                }
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

                return new PathFinderResult
                {
                    Edges = list,
                    Weight = weight
                };
            }
        }
    }
}