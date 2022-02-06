﻿using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using Npgsql;
using NpgsqlTypes;
using Placium.Route.Common;

namespace Placium.Route.Algorithms;

public class MemoryAStar : BasePathFinderAlgorithm
{
    public MemoryAStar(Guid guid, string connectionString, string vehicleType, string profile, float minFactor,
        float maxFactor) :
        base(guid,
            connectionString, vehicleType, profile, minFactor, maxFactor)
    {
    }


    public override async Task<PathFinderResult> FindPathAsync(RouterPoint source,
        RouterPoint target, float maxWeight = float.MaxValue)
    {
        var stopWatch = new Stopwatch();

        stopWatch.Start();

        var minWeight = MinFactor * 1;
        var size = 0.01f;

        await using var connection = new SqliteConnection("Data source=:memory:");
        await using var connection2 = new NpgsqlConnection(ConnectionString);
        await connection.OpenAsync();
        await connection2.OpenAsync();


        connection.CreateFunction(
            "distanceInMeters",
            (double lat1, double lon1, double lat2, double lon2) =>
            {
                const double r = 6371000; // metres
                var φ1 = lat1 * Math.PI / 180; // φ, λ in radians
                var φ2 = lat2 * Math.PI / 180;
                var δφ = (lat2 - lat1) * Math.PI / 180;
                var δλ = (lon2 - lon1) * Math.PI / 180;

                var a = Math.Pow(Math.Sin(δφ / 2), 2) +
                        Math.Cos(φ1) * Math.Cos(φ2) *
                        Math.Pow(Math.Sin(δλ / 2), 2);
                var c = 2 * Math.Atan2(Math.Sqrt(a), Math.Sqrt(1 - a));

                return r * c; // in metres
            });

        await using (var command =
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
            await command.PrepareAsync();
            await command.ExecuteNonQueryAsync();
        }

        await using (var command =
                     new SqliteCommand(string.Join(";", "PRAGMA synchronous = OFF",
                         @"CREATE TEMP TABLE temp_prefetch (
	                id INTEGER PRIMARY KEY NOT NULL, 
	                latitude REAL NOT NULL, 
	                longitude REAL NOT NULL
                )", @"CREATE TEMP TABLE temp_node (
	                id INTEGER PRIMARY KEY NOT NULL, 
	                latitude REAL NOT NULL, 
	                longitude REAL NOT NULL, 
	                from_weight REAL NOT NULL
                )", @"CREATE TEMP TABLE temp_edge (
	                id INTEGER PRIMARY KEY NOT NULL, 
	                from_node INTEGER NOT NULL, 
	                to_node INTEGER NOT NULL,
	                weight REAL NOT NULL,
                    direction INTEGER NOT NULL
                )", @"CREATE TEMP TABLE temp_dijkstra (
	                node INTEGER PRIMARY KEY NOT NULL, 
	                weight REAL NOT NULL, 
	                g REAL NOT NULL, 
	                edge INTEGER NULL,
	                in_queue INTEGER NOT NULL,
                    FOREIGN KEY(node) REFERENCES temp_node(id),
                    FOREIGN KEY(edge) REFERENCES temp_edge(id)
                )", @"CREATE TEMP TABLE temp_restriction (
	                id INTEGER PRIMARY KEY NOT NULL,
	                from_edge INTEGER NOT NULL,
	                to_edge INTEGER NOT NULL,
	                via_node INTEGER NOT NULL
                )"), connection))
        {
            command.Prepare();
            await command.ExecuteNonQueryAsync();
        }

        await using (var command =
                     new SqliteCommand(string.Join(";",
                             @"CREATE INDEX temp_prefetch_latitude_idx ON temp_prefetch (latitude)",
                             @"CREATE INDEX temp_prefetch_longitude_idx ON temp_prefetch (longitude)",
                             @"CREATE INDEX temp_node_latitude_idx ON temp_node (latitude)",
                             @"CREATE INDEX temp_node_longitude_idx ON temp_node (longitude)",
                             @"CREATE INDEX temp_dijkstra_in_queue_idx ON temp_dijkstra (in_queue)",
                             @"CREATE INDEX temp_dijkstra_weight_idx ON temp_dijkstra (weight)",
                             @"CREATE INDEX temp_edge_from_node_to_node_idx ON temp_edge (from_node,to_node)",
                             @"CREATE UNIQUE INDEX temp_restriction_from_edge_to_edge_via_node_idx ON temp_restriction (from_edge,to_edge,via_node)"),
                         connection))
        {
            command.Prepare();
            await command.ExecuteNonQueryAsync();
        }


        await using var commandBegin =
            new SqliteCommand(@"BEGIN TRANSACTION",
                connection);
        await using var commandCommit =
            new SqliteCommand(@"COMMIT",
                connection);

        commandBegin.Prepare();
        commandCommit.Prepare();

        await using var commandInsertIntoRestriction =
            new SqliteCommand(@"INSERT INTO temp_restriction(id,from_edge,to_edge,via_node)
                VALUES (@id,@fromEdge,@toEdge,@viaNode)
                ON CONFLICT (from_edge,to_edge,via_node) DO NOTHING",
                connection);

        commandInsertIntoRestriction.Parameters.Add("id", SqliteType.Integer);
        commandInsertIntoRestriction.Parameters.Add("fromEdge", SqliteType.Integer);
        commandInsertIntoRestriction.Parameters.Add("toEdge", SqliteType.Integer);
        commandInsertIntoRestriction.Parameters.Add("viaNode", SqliteType.Integer);
        commandInsertIntoRestriction.Prepare();

        await using var commandSelectFromPrefedched = new SqliteCommand(
            @"WITH cte AS (SELECT id,latitude,longitude FROM temp_node WHERE id=@node),
                cte1 AS (SELECT p.id FROM temp_prefetch p JOIN cte n ON p.latitude<=n.latitude+@size),
                cte2 AS (SELECT p.id FROM temp_prefetch p JOIN cte n ON p.longitude<=n.longitude+@size),
                cte3 AS (SELECT p.id FROM temp_prefetch p JOIN cte n ON p.latitude>=n.latitude-@size),
                cte4 AS (SELECT p.id FROM temp_prefetch p JOIN cte n ON p.longitude>=n.longitude-@size)
                SELECT EXISTS (SELECT 1 FROM cte1 JOIN cte2 ON cte1.id=cte2.id JOIN cte3 ON cte1.id=cte3.id JOIN cte4 ON cte1.id=cte4.id)",
            connection);

        commandSelectFromPrefedched.Parameters.Add("node", SqliteType.Integer);
        commandSelectFromPrefedched.Parameters.AddWithValue("size", size);
        commandSelectFromPrefedched.Prepare();

        await using var commandInsertIntoPrefedched = new SqliteCommand(
            @"INSERT INTO temp_prefetch (id,latitude,longitude) VALUES (@id,@latitude,@longitude) ON CONFLICT DO NOTHING",
            connection);

        commandInsertIntoPrefedched.Parameters.Add("id", SqliteType.Integer);
        commandInsertIntoPrefedched.Parameters.Add("latitude", SqliteType.Real);
        commandInsertIntoPrefedched.Parameters.Add("longitude", SqliteType.Real);
        commandInsertIntoPrefedched.Prepare();

        await using var commandInsertIntoNode = new SqliteCommand(
            @"INSERT INTO temp_node (id,latitude,longitude,from_weight) 
                VALUES (@id,@latitude,@longitude,
                    @factor*distanceInMeters(@latitude,@longitude,@fromLatitude,@fromLongitude))
                ON CONFLICT (id) DO NOTHING",
            connection);
        await using var commandInsertIntoEdge = new SqliteCommand(
            @"INSERT INTO temp_edge (id,from_node,to_node,weight,direction)
                VALUES (@id,@fromNode,@toNode,@weight,@direction)
                ON CONFLICT (id) DO NOTHING",
            connection);

        await using var commandSelectFromNode =
            new NpgsqlCommand(string.Join(";",
                    @"SELECT id,latitude,longitude FROM node WHERE id=@node",
                    @"WITH cte AS (SELECT id,latitude,longitude FROM node WHERE id=@node AND guid=@guid),
                        cte2 AS (SELECT n2.id FROM node n2 JOIN cte n1
                        ON n2.latitude<=n1.latitude+@size 
                        AND n2.longitude<=n1.longitude+@size 
                        AND n2.latitude>=n1.latitude-@size 
                        AND n2.longitude>=n1.longitude-@size
                        WHERE n2.guid=@guid)
                    SELECT n.id,n.latitude,n.longitude
                    FROM node n JOIN edge e ON n.id=e.from_node JOIN cte2 n2 ON n2.id=e.to_node
                    WHERE n.guid=@guid AND e.guid=@guid
                    UNION ALL SELECT n.id,n.latitude,n.longitude
                    FROM node n JOIN edge e ON n.id=e.to_node JOIN cte2 n2 ON n2.id=e.from_node
                    WHERE n.guid=@guid AND e.guid=@guid",
                    @"WITH cte AS (SELECT id,latitude,longitude FROM node WHERE id=@node AND guid=@guid),
                        cte2 AS (SELECT n2.id FROM node n2 JOIN cte n1
                        ON n2.latitude<=n1.latitude+@size 
                        AND n2.longitude<=n1.longitude+@size 
                        AND n2.latitude>=n1.latitude-@size 
                        AND n2.longitude>=n1.longitude-@size
                        WHERE n2.guid=@guid)
                    SELECT e.id,e.from_node,e.to_node,
                    GREATEST((weight->@profile)::real,@minWeight),(direction->@profile)::smallint
                    FROM edge e JOIN cte2 n2 ON e.from_node=n2.id OR e.to_node=n2.id
                    WHERE weight?@profile AND direction?@profile AND e.guid=@guid",
                    @"WITH cte AS (SELECT id,latitude,longitude FROM node WHERE id=@node AND guid=@guid),
                        cte2 AS (SELECT n2.id FROM node n2 JOIN cte n1
                        ON n2.latitude<=n1.latitude+@size 
                        AND n2.longitude<=n1.longitude+@size 
                        AND n2.latitude>=n1.latitude-@size 
                        AND n2.longitude>=n1.longitude-@size
                        WHERE n2.guid=@guid)
                    SELECT r.id,r.from_edge,r.to_edge,r.via_node FROM restriction r 
                    JOIN edge e ON r.from_edge=e.id OR r.to_edge=e.id JOIN cte2 n2 ON e.from_node=n2.id OR e.to_node=n2.id
                    WHERE r.vehicle_type=@vehicleType AND r.guid=@guid AND e.guid=@guid
                    UNION ALL SELECT r.id,r.from_edge,r.to_edge,r.via_node FROM restriction r 
                    JOIN cte2 n2 ON r.via_node=n2.id
                    WHERE r.vehicle_type=@vehicleType AND r.guid=@guid"),
                connection2);

        commandInsertIntoNode.Parameters.Add("id", SqliteType.Integer);
        commandInsertIntoNode.Parameters.Add("latitude", SqliteType.Real);
        commandInsertIntoNode.Parameters.Add("longitude", SqliteType.Real);
        commandInsertIntoNode.Parameters.AddWithValue("fromLatitude", source.Coordinate.Latitude);
        commandInsertIntoNode.Parameters.AddWithValue("fromLongitude", source.Coordinate.Longitude);
        commandInsertIntoNode.Parameters.AddWithValue("factor", MinFactor);
        commandInsertIntoNode.Prepare();

        commandInsertIntoEdge.Parameters.Add("id", SqliteType.Integer);
        commandInsertIntoEdge.Parameters.Add("fromNode", SqliteType.Integer);
        commandInsertIntoEdge.Parameters.Add("toNode", SqliteType.Integer);
        commandInsertIntoEdge.Parameters.Add("weight", SqliteType.Real);
        commandInsertIntoEdge.Parameters.Add("direction", SqliteType.Integer);
        commandInsertIntoEdge.Prepare();

        commandSelectFromNode.Parameters.AddWithValue("minWeight", minWeight);
        commandSelectFromNode.Parameters.AddWithValue("vehicleType", VehicleType);
        commandSelectFromNode.Parameters.AddWithValue("profile", Profile);
        commandSelectFromNode.Parameters.AddWithValue("guid", Guid);
        commandSelectFromNode.Parameters.AddWithValue("size", size);
        commandSelectFromNode.Parameters.Add("node", NpgsqlDbType.Bigint);
        commandSelectFromNode.Prepare();

        void LoadEdgesAndNodes(long node)
        {
            commandSelectFromPrefedched.Parameters["node"].Value = node;

            if ((long)commandSelectFromPrefedched.ExecuteScalar()! != 0)
                return;

            commandSelectFromNode.Parameters["node"].Value = node;

            commandBegin.ExecuteNonQuery();

            using (var reader = commandSelectFromNode.ExecuteReader())
            {
                while (reader.Read())
                {
                    commandInsertIntoPrefedched.Parameters["id"].Value = reader.GetInt64(0);
                    commandInsertIntoPrefedched.Parameters["latitude"].Value = reader.GetFloat(1);
                    commandInsertIntoPrefedched.Parameters["longitude"].Value = reader.GetFloat(2);

                    commandInsertIntoPrefedched.ExecuteNonQuery();
                }

                reader.NextResult();

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

                reader.NextResult();


                while (reader.Read())
                {
                    commandInsertIntoRestriction.Parameters["id"].Value = reader.GetInt64(0);
                    commandInsertIntoRestriction.Parameters["fromEdge"].Value = reader.GetInt64(1);
                    commandInsertIntoRestriction.Parameters["toEdge"].Value = reader.GetInt64(2);
                    commandInsertIntoRestriction.Parameters["viaNode"].Value = reader.GetInt64(3);
                    commandInsertIntoRestriction.ExecuteNonQuery();
                }
            }

            commandCommit.ExecuteNonQuery();
        }

        await using (var command =
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
	                null,
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

            if (new[] { 0, 1, 3, 4 }.Contains(target.Direction))
            {
                LoadEdgesAndNodes(target.FromNode);
                command.Parameters["node"].Value = target.FromNode;
                var coords = target.Coordinates.First();
                command.Parameters["latitude1"].Value = coords.Latitude;
                command.Parameters["longitude1"].Value = coords.Longitude;
                await command.ExecuteNonQueryAsync();
            }

            if (new[] { 0, 2, 3, 5 }.Contains(target.Direction))
            {
                LoadEdgesAndNodes(target.ToNode);
                command.Parameters["node"].Value = target.ToNode;
                var coords = target.Coordinates.Last();
                command.Parameters["latitude1"].Value = coords.Latitude;
                command.Parameters["longitude1"].Value = coords.Longitude;
                await command.ExecuteNonQueryAsync();
            }
        }

        var steps = 0L;
        var node = 0L;
        float? weight = null;


        var sources = new List<long>();
        if (new[] { 0, 1, 3, 4 }.Contains(target.Direction)) sources.Add(source.ToNode);
        if (new[] { 0, 2, 3, 5 }.Contains(target.Direction)) sources.Add(source.FromNode);

        await using (var command =
                     new SqliteCommand(string.Join(";",
                             @"SELECT node,weight,NOT in_queue FROM temp_dijkstra WHERE node=@sourceFirst OR node=@sourceLast ORDER BY weight LIMIT 1",
                             @"SELECT node FROM temp_dijkstra WHERE in_queue ORDER BY weight LIMIT 1"),
                         connection))
        await using (var command2 =
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
                            AND NOT EXISTS (SELECT * FROM temp_restriction WHERE via_node=t.node AND to_edge=t.edge AND from_edge=e.id)
                            UNION ALL SELECT e.to_node AS node,n.from_weight+t.g+e.weight AS weight,t.g+e.weight AS g,e.id AS edge,1 AS in_queue
		                    FROM temp_edge e JOIN temp_node n ON e.to_node=n.id JOIN temp_dijkstra t ON e.from_node=t.node
                            WHERE (e.direction=0 OR e.direction=2 OR e.direction=3 OR e.direction=5) AND t.node=@node
                            AND NOT EXISTS (SELECT * FROM temp_restriction WHERE via_node=t.node AND to_edge=t.edge AND from_edge=e.id)) q
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

            for (;; steps++)
            {
                var node1 = 0L;

                await using (var reader = await command.ExecuteReaderAsync())
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
            }
        }

        await using (var command =
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
                await using var reader = await command.ExecuteReaderAsync();

                if (!reader.Read()) break;

                node = reader.GetInt64(0);
                var edge = reader.GetInt64(1);
                list.Add(edge);
            }

            stopWatch.Stop();

            Console.WriteLine(
                $"{nameof(MemoryAStar)} steps={steps} ElapsedMilliseconds={stopWatch.ElapsedMilliseconds}");


            return new PathFinderResult
            {
                Edges = list,
                Weight = weight
            };
        }
    }
}