using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using OsmSharp;
using OsmSharp.Tags;
using Placium.Common;
using Placium.Route.Osm.Vehicles;
using Placium.Route.Restructions;
using Placium.Types;
using Route.LocalGeo;

namespace Placium.Route;

public class RouterDb
{
    public RouterDb(Guid guid, string connectionString,
        Car vehicle)
    {
        Guid = guid;
        ConnectionString = connectionString;
        Vehicle = vehicle;
    }

    public Guid Guid { get; }
    public string ConnectionString { get; }

    public Car Vehicle { get; }

    public async Task LoadFromOsmAsync(string osmConnectionString, IProgressClient progressClient, string session)
    {
        using var osmConnection = new NpgsqlConnection(osmConnectionString);
        using var connection = new NpgsqlConnection(ConnectionString);
        using var connection2 = new NpgsqlConnection(ConnectionString);

        await osmConnection.OpenAsync();
        await connection.OpenAsync();
        await connection2.OpenAsync();

        osmConnection.ReloadTypes();
        osmConnection.TypeMapper.MapComposite<OsmRelationMember>("relation_member");
        osmConnection.TypeMapper.MapEnum<OsmType>("osm_type");

        var id = string.Empty;

        await ExecuteResourceAsync(Assembly.GetExecutingAssembly(),
            "Placium.Route.CreateTempTables.pgsql",
            connection);
        await ExecuteResourceAsync(Assembly.GetExecutingAssembly(),
            "Placium.Route.CreateTempTables2.pgsql",
            connection2);
        using (var writer2 = connection2.BeginTextImport(
                   "COPY temp_restriction (guid,vehicle_type,from_way,to_way,via_node,tags) FROM STDIN WITH NULL AS ''")
              )
        {
            void FoundRestriction(string vehicleType, long fromWay, long toWay, long viaNode,
                TagsCollectionBase tags)
            {
                if (vehicleType == null) vehicleType = string.Empty;

                var values = new[]
                {
                    Guid.ToString(),
                    vehicleType,
                    fromWay.ToString(),
                    toWay.ToString(),
                    viaNode.ToString(),
                    $"{string.Join(",", tags.Select(t => $"\"{t.Key.TextEscape(2)}\"=>\"{t.Value.TextEscape(2)}\""))}"
                };

                writer2.WriteLine(string.Join("\t", values));
            }

            id = Guid.NewGuid().ToString();
            await progressClient.Init(id, session);

            using (var command0 = new NpgsqlCommand(string.Join(";",
                       @"ANALYZE node"), osmConnection))
            using (var command = new NpgsqlCommand(string.Join(";",
                       @"SELECT reltuples::bigint AS estimate FROM pg_class WHERE relname = 'node'",
                       @"SELECT
	                            id,
	                            version,
	                            latitude,
	                            longitude,
	                            change_set_id,
	                            time_stamp,
	                            user_id,
	                            user_name,
	                            visible,
	                            tags
                        FROM node"), osmConnection))
            {
                command0.Prepare();
                command.Prepare();

                await command0.ExecuteNonQueryAsync();
                using var reader = await command.ExecuteReaderAsync();
                if (!reader.Read()) throw new NullReferenceException();
                var count = reader.GetInt64(0);
                await reader.NextResultAsync();
                var current = 0L;
                var objReader = new object();
                var objFoundRestriction = new object();
                var objProgress = new object();
                var doIt = true;

                await ExecuteResourceAsync(Assembly.GetExecutingAssembly(),
                    "Placium.Route.DropIndices.pgsql",
                    connection);

                Parallel.For(0, 6, i =>
                {
                    using var osmConnection2 = new NpgsqlConnection(osmConnectionString);
                    using var connection3 = new NpgsqlConnection(ConnectionString);
                    osmConnection2.Open();
                    connection3.Open();

                    using var command2 = new NpgsqlCommand(@"SELECT 
	                            id,
	                            version,
	                            change_set_id,
	                            time_stamp,
	                            user_id,
	                            user_name,
	                            visible,
	                            tags,
	                            nodes
                            FROM way WHERE @ids && nodes", osmConnection2);

                    command2.Parameters.Add("ids", NpgsqlDbType.Array | NpgsqlDbType.Bigint);

                    command2.Prepare();

                    var take = 1000;

                    for (;;)
                    {
                        var nodes = new List<Node>(take);
                        lock (objReader)
                        {
                            if (!doIt) break;
                            for (var j = 0; j < take; j++)
                            {
                                doIt = reader.Read();
                                if (!doIt) break;
                                nodes.Add(new Node().Fill(reader));
                            }

                            if (!nodes.Any()) break;
                        }

                        command2.Parameters["ids"].Value = nodes.Select(x => x.Id.Value).ToArray();

                        var ways = new List<Way>(take);
                        using (var reader2 = command2.ExecuteReader())
                        {
                            while (reader2.Read())
                            {
                                var way = new Way().Fill(reader2);
                                var wayTags = way.Tags;
                                if (Vehicle.CanTraverse(wayTags)) ways.Add(way);
                            }
                        }

                        using (var writer = connection3.BeginTextImport(
                                   "COPY node (guid,id,latitude,longitude,tags) FROM STDIN WITH NULL AS ''"))
                        {
                            foreach (var node in nodes)
                            {
                                var list = ways.Where(x => x.Nodes.Contains(node.Id.Value)).ToList();
                                if (list.Any())
                                {
                                    var is_core = list.Any(way =>
                                        node.Id == way.Nodes.First() || node.Id == way.Nodes.Last());

                                    var nodeTags = node.Tags;
                                    if (nodeTags != null &&
                                        (nodeTags.Contains("barrier", "bollard") ||
                                         nodeTags.Contains("barrier", "fence") ||
                                         nodeTags.Contains("barrier", "gate")))
                                    {
                                        is_core = true;
                                        lock (objFoundRestriction)
                                        {
                                            foreach (var way in list)
                                                FoundRestriction("motorcar", way.Id.Value, way.Id.Value,
                                                    node.Id.Value,
                                                    nodeTags);
                                        }
                                    }
                                    else
                                    {
                                        var nodeRestriction = Vehicle.NodeRestriction(nodeTags);
                                        if (nodeRestriction != null &&
                                            !string.IsNullOrEmpty(nodeRestriction.Vehicle))
                                        {
                                            is_core = true;

                                            lock (objFoundRestriction)
                                            {
                                                foreach (var way in list)
                                                    FoundRestriction(nodeRestriction.Vehicle, way.Id.Value,
                                                        way.Id.Value,
                                                        node.Id.Value,
                                                        nodeTags);
                                            }
                                        }
                                    }

                                    if (!is_core && nodeTags != null && nodeTags.Any() &&
                                        Vehicle.NodeTagProcessor(nodeTags))
                                        is_core = true;

                                    if (is_core)
                                    {
                                        var values = new[]
                                        {
                                            Guid.ToString(),
                                            node.Id.ToString(),
                                            node.Latitude.ValueAsText(),
                                            node.Longitude.ValueAsText(),
                                            $"{string.Join(",", node.Tags.Select(t => $"\"{t.Key.TextEscape(2)}\"=>\"{t.Value.TextEscape(2)}\""))}"
                                        };

                                        writer.WriteLine(string.Join("\t", values));
                                    }
                                }
                            }
                        }

                        lock (objProgress)
                        {
                            current += nodes.Count;
                            progressClient.Progress(100f * current / count, id, session).GetAwaiter()
                                .GetResult();
                        }
                    }

                    osmConnection2.Close();
                    connection3.Close();
                });
            }

            await ExecuteResourceAsync(Assembly.GetExecutingAssembly(),
                "Placium.Route.CreateIndices.pgsql",
                connection);

            await progressClient.Finalize(id, session);


            id = Guid.NewGuid().ToString();
            await progressClient.Init(id, session);

            using (var command = new NpgsqlCommand(string.Join("; ", @"SELECT COUNT(*) 
                            FROM relation r,unnest(r.members) m WHERE m.role='via'", @"SELECT
	                            r.id,
	                            r.version,
	                            r.change_set_id,
	                            r.time_stamp,
	                            r.user_id,
	                            r.user_name,
	                            r.visible,
	                            r.tags,
	                            r.members,
                                w.nodes
                            FROM relation r,unnest(r.members) m JOIN way w ON m.id=w.id WHERE m.type=2 AND m.role='via'
                            UNION ALL SELECT
	                            r.id,
	                            r.version,
	                            r.change_set_id,
	                            r.time_stamp,
	                            r.user_id,
	                            r.user_name,
	                            r.visible,
	                            r.tags,
	                            r.members,
                                ARRAY[m.id]
                            FROM relation r,unnest(r.members) m WHERE m.type=1 AND m.role='via'"), osmConnection))
            {
                command.Prepare();

                using var reader = await command.ExecuteReaderAsync();
                if (!reader.Read()) throw new NullReferenceException();
                var count = reader.GetInt64(0);
                var current = 0L;
                await reader.NextResultAsync();
                while (reader.Read())
                {
                    var relation = new Relation().Fill(reader);
                    var via = (long[])reader.GetValue(9);

                    if (via.Length != 1) continue;

                    var viaNode = via.First();

                    if (relation.IsRestriction(out var vehicleType) &&
                        relation.Members != null)
                    {
                        if (string.IsNullOrWhiteSpace(vehicleType)) vehicleType = "motorcar";

                        var type = "restriction";
                        if (!string.IsNullOrWhiteSpace(vehicleType)) type = type + ":" + vehicleType;

                        foreach (var fromWay in relation.Members
                                     .Where(m => m.Role == "from" && m.Type == OsmGeoType.Way).ToList())
                        foreach (var toWay in relation.Members
                                     .Where(m => m.Role == "to" && m.Type == OsmGeoType.Way).ToList())
                            FoundRestriction(vehicleType, fromWay.Id, toWay.Id, viaNode, new TagsCollection(
                                new Tag("type", type),
                                new Tag("restriction", "no_turn")));
                    }

                    if (current++ % 1000 == 0)
                        await progressClient.Progress(100f * current / count, id, session);
                }
            }

            await progressClient.Finalize(id, session);

            id = Guid.NewGuid().ToString();
            await progressClient.Init(id, session);

            await ExecuteResourceAsync(Assembly.GetExecutingAssembly(),
                "Placium.Route.CreateTempTables3.pgsql",
                connection);


            using (var command = new NpgsqlCommand(string.Join(";", @"SELECT COUNT(*) 
                            FROM way w1 JOIN way w2 ON w1.nodes&&w2.nodes WHERE w1.id<w2.id", @"SELECT
	                            w1.id,
	                            w1.version,
	                            w1.change_set_id,
	                            w1.time_stamp,
	                            w1.user_id,
	                            w1.user_name,
	                            w1.visible,
	                            w1.tags,
	                            w1.nodes,
	                            w2.id,
	                            w2.version,
	                            w2.change_set_id,
	                            w2.time_stamp,
	                            w2.user_id,
	                            w2.user_name,
	                            w2.visible,
	                            w2.tags,
	                            w2.nodes
                            FROM way w1 JOIN way w2 ON w1.nodes&&w2.nodes WHERE w1.id<w2.id"), osmConnection))
            {
                command.Prepare();

                var vehicleType = string.Empty;
                if (string.IsNullOrWhiteSpace(vehicleType)) vehicleType = "motorcar";

                var type = "restriction";
                if (!string.IsNullOrWhiteSpace(vehicleType)) type = type + ":" + vehicleType;

                using var reader = await command.ExecuteReaderAsync();
                if (!reader.Read()) throw new NullReferenceException();
                var count = reader.GetInt64(0);
                await reader.NextResultAsync();
                var current = 0L;
                var objReader = new object();
                var objFoundRestriction = new object();
                var objProgress = new object();
                var doIt = true;

                Parallel.For(0, 6, i =>
                {
                    using var osmConnection2 = new NpgsqlConnection(osmConnectionString);
                    osmConnection2.Open();
                    using var command2 = new NpgsqlCommand(@"SELECT 
	                            id,
	                            version,
	                            latitude,
	                            longitude,
	                            change_set_id,
	                            time_stamp,
	                            user_id,
	                            user_name,
	                            visible,
	                            tags
                            FROM node WHERE id=ANY(@ids)", osmConnection2);
                    command2.Parameters.Add("ids", NpgsqlDbType.Array | NpgsqlDbType.Bigint);
                    command2.Prepare();

                    var way1 = new Way();
                    var way2 = new Way();

                    for (;;)
                    {
                        lock (objReader)
                        {
                            if (!doIt) break;
                            doIt = reader.Read();
                            if (!doIt) break;
                            way1.Fill(reader);
                            way2.Fill(reader, 9);
                        }

                        var attributes1 = way1.Tags;
                        var attributes2 = way2.Tags;

                        if (way1.Nodes.Length > 1 && way2.Nodes.Length > 1 &&
                            Vehicle.CanTraverse(attributes1) &&
                            Vehicle.CanTraverse(attributes2))
                        {
                            var ids = new[]
                            {
                                way1.Nodes[0],
                                way1.Nodes[1],
                                way1.Nodes[^1],
                                way1.Nodes[^2],
                                way2.Nodes[0],
                                way2.Nodes[1],
                                way2.Nodes[^1],
                                way2.Nodes[^2]
                            };

                            command2.Parameters["ids"].Value = ids;

                            var list = new List<Node>(8);
                            using var reader2 = command2.ExecuteReader();

                            while (reader2.Read())
                            {
                                var node = new Node().Fill(reader2);
                                list.Add(node);
                            }

                            var dictionary = list.ToDictionary(x => x.Id,
                                x => new Coordinate((float)x.Latitude, (float)x.Longitude));
                            if (way1.Nodes.First() == way2.Nodes.First())
                            {
                                var fromCoords = dictionary[way1.Nodes[1]];
                                var toCoords = dictionary[way2.Nodes[1]];
                                var viaCoords = dictionary[way1.Nodes[0]];

                                if (Coordinate.AngleInDegree(fromCoords, toCoords, viaCoords) < 60f)
                                    lock (objFoundRestriction)
                                    {
                                        FoundRestriction(vehicleType, way1.Id.Value,
                                            way2.Id.Value, way1.Nodes[0],
                                            new TagsCollection(
                                                new Tag("type", type),
                                                new Tag("restriction", "no_turn")));
                                        FoundRestriction(vehicleType, way2.Id.Value, way1.Id.Value,
                                            way1.Nodes[0],
                                            new TagsCollection(
                                                new Tag("type", type),
                                                new Tag("restriction", "no_turn")));
                                    }
                            }

                            if (way1.Nodes.First() == way2.Nodes.Last())
                            {
                                var fromCoords = dictionary[way1.Nodes[1]];
                                var toCoords = dictionary[way2.Nodes[^2]];
                                var viaCoords = dictionary[way1.Nodes[0]];

                                if (Coordinate.AngleInDegree(fromCoords, toCoords, viaCoords) < 60f)
                                    lock (objFoundRestriction)
                                    {
                                        FoundRestriction(vehicleType, way1.Id.Value,
                                            way2.Id.Value, way1.Nodes[0],
                                            new TagsCollection(
                                                new Tag("type", type),
                                                new Tag("restriction", "no_turn")));
                                        FoundRestriction(vehicleType, way2.Id.Value, way1.Id.Value,
                                            way1.Nodes[0],
                                            new TagsCollection(
                                                new Tag("type", type),
                                                new Tag("restriction", "no_turn")));
                                    }
                            }

                            if (way1.Nodes.Last() == way2.Nodes.Last())
                            {
                                var fromCoords = dictionary[way1.Nodes[^2]];
                                var toCoords = dictionary[way2.Nodes[^2]];
                                var viaCoords = dictionary[way1.Nodes[^1]];

                                if (Coordinate.AngleInDegree(fromCoords, toCoords, viaCoords) < 60f)
                                    lock (objFoundRestriction)
                                    {
                                        FoundRestriction(vehicleType, way1.Id.Value,
                                            way2.Id.Value, way1.Nodes[^1],
                                            new TagsCollection(
                                                new Tag("type", type),
                                                new Tag("restriction", "no_turn")));
                                        FoundRestriction(vehicleType, way2.Id.Value, way1.Id.Value,
                                            way1.Nodes[^1],
                                            new TagsCollection(
                                                new Tag("type", type),
                                                new Tag("restriction", "no_turn")));
                                    }
                            }

                            if (way1.Nodes.Last() == way2.Nodes.First())
                            {
                                var fromCoords = dictionary[way1.Nodes[^2]];
                                var toCoords = dictionary[way2.Nodes[1]];
                                var viaCoords = dictionary[way2.Nodes[0]];

                                if (Coordinate.AngleInDegree(fromCoords, toCoords, viaCoords) < 60f)
                                    lock (objFoundRestriction)
                                    {
                                        FoundRestriction(vehicleType, way1.Id.Value,
                                            way2.Id.Value, way2.Nodes[0],
                                            new TagsCollection(
                                                new Tag("type", type),
                                                new Tag("restriction", "no_turn")));
                                        FoundRestriction(vehicleType, way2.Id.Value, way1.Id.Value,
                                            way2.Nodes[0],
                                            new TagsCollection(
                                                new Tag("type", type),
                                                new Tag("restriction", "no_turn")));
                                    }
                            }
                        }

                        lock (objProgress)
                        {
                            if (current++ % 1000 == 0)
                                progressClient.Progress(100f * current / count, id, session).GetAwaiter()
                                    .GetResult();
                        }
                    }

                    osmConnection2.Close();
                });
            }

            await progressClient.Finalize(id, session);

            id = Guid.NewGuid().ToString();
            await progressClient.Init(id, session);

            using (var writer = connection.BeginTextImport(
                       @"COPY temp_edge (guid,from_node,to_node,way,
	                    from_latitude, 
	                    from_longitude, 
	                    to_latitude, 
	                    to_longitude,
                        distance,coordinates,location,tags,direction,weight) FROM STDIN WITH NULL AS ''")
                  )
            using (var command0 = new NpgsqlCommand(string.Join(";",
                       @"ANALYZE way"), osmConnection))
            using (var command = new NpgsqlCommand(string.Join(";",
                       @"SELECT reltuples::bigint AS estimate FROM pg_class WHERE relname = 'way'",
                       @"SELECT
                        id,
                        version,
                        change_set_id,
                        time_stamp,
                        user_id,
                        user_name,
                        visible,
                        tags,
                        nodes
                        FROM way"), osmConnection))
            {
                command0.Prepare();
                command.Prepare();

                await command0.ExecuteNonQueryAsync();
                using var reader = await command.ExecuteReaderAsync();
                if (!reader.Read()) throw new NullReferenceException();
                var count = reader.GetInt64(0);
                await reader.NextResultAsync();
                var current = 0L;
                var objReader = new object();
                var objWriter = new object();
                var objProgress = new object();
                var doIt = true;

                Parallel.For(0, 6, j =>
                {
                    using var osmConnection2 = new NpgsqlConnection(osmConnectionString);
                    using var connection3 = new NpgsqlConnection(ConnectionString);
                    osmConnection2.Open();
                    connection3.Open();

                    using var command3 = new NpgsqlCommand(
                        @"SELECT id FROM node WHERE id=ANY(@ids) AND guid=@guid",
                        connection3);
                    using var command4 = new NpgsqlCommand(
                        @"SELECT id,latitude,longitude FROM node WHERE id=ANY(@ids)",
                        osmConnection2);

                    command3.Parameters.Add("ids", NpgsqlDbType.Array | NpgsqlDbType.Bigint);
                    command3.Parameters.AddWithValue("guid", Guid);
                    command3.Prepare();
                    command4.Parameters.Add("ids", NpgsqlDbType.Array | NpgsqlDbType.Bigint);
                    command4.Prepare();

                    var way = new Way();
                    for (;;)
                    {
                        lock (objReader)
                        {
                            if (!doIt) break;
                            doIt = reader.Read();
                            if (!doIt) break;
                            way.Fill(reader);
                        }

                        var attributes = way.Tags;

                        if (Vehicle.CanTraverse(attributes))
                        {
                            var factorAndSpeeds = Vehicle.FactorAndSpeeds(attributes);

                            command3.Parameters["ids"].Value = way.Nodes;
                            command4.Parameters["ids"].Value = way.Nodes;

                            var list = new List<NodeItem>(way.Nodes.Length);
                            var core = new List<long>(way.Nodes.Length);

                            using (var reader4 = command4.ExecuteReader())
                            {
                                while (reader4.Read())
                                    list.Add(new NodeItem
                                    {
                                        Id = reader4.GetInt64(0),
                                        Latitude = reader4.GetFloat(1),
                                        Longitude = reader4.GetFloat(2)
                                    });
                            }

                            using (var reader3 = command3.ExecuteReader())
                            {
                                while (reader3.Read())
                                    core.Add(reader3.GetInt64(0));
                            }

                            var dictionary = list.ToDictionary(item => item.Id, item => item);

                            // convert way into one or more edges.
                            var i = 0;

                            while (i < way.Nodes.Length - 1)
                            {
                                // build edge to add.
                                var intermediates = new List<Coordinate>();
                                var distance = 0.0f;
                                if (!dictionary.TryGetValue(way.Nodes[i], out var item)) break;


                                var previousCoordinate = new Coordinate(item.Latitude, item.Longitude);
                                intermediates.Add(previousCoordinate);

                                var fromNode = way.Nodes[i];
                                i++;

                                var toNode = (long?)null;
                                for (;;)
                                {
                                    if (i >= way.Nodes.Length ||
                                        !dictionary.TryGetValue(way.Nodes[i], out item))
                                        // an incomplete way, node not in source.
                                        break;

                                    var coordinate = new Coordinate(item.Latitude, item.Longitude);

                                    distance += Coordinate.DistanceEstimateInMeter(
                                        previousCoordinate, coordinate);

                                    intermediates.Add(coordinate);
                                    previousCoordinate = coordinate;

                                    if (core.Contains(item.Id))
                                    {
                                        // node is part of the core.
                                        toNode = way.Nodes[i];
                                        break;
                                    }

                                    i++;
                                }

                                if (toNode == null) break;

                                var direction = factorAndSpeeds.ToDictionary(x => x.Key, x => x.Value.Direction);
                                var weight = factorAndSpeeds.Where(x => x.Value.Factor > 0)
                                    .ToDictionary(x => x.Key, x => distance * x.Value.Factor);

                                var fromCoords = intermediates.First();
                                var toCoords = intermediates.Last();

                                var values = new[]
                                {
                                    Guid.ToString(),
                                    fromNode.ToString(),
                                    toNode.ToString(),
                                    way.Id.ToString(),
                                    fromCoords.Latitude.ValueAsText(),
                                    fromCoords.Longitude.ValueAsText(),
                                    toCoords.Latitude.ValueAsText(),
                                    toCoords.Longitude.ValueAsText(),
                                    distance.ValueAsText(),
                                    $"{{{string.Join(",", intermediates.Select(t => $"\\\"({t.Latitude.ValueAsText()},{t.Longitude.ValueAsText()})\\\""))}}}",
                                    intermediates.Count switch
                                    {
                                        0 => "SRID=4326;POINT EMPTY",
                                        1 =>
                                            $"SRID=4326;POINT({string.Join(",", intermediates.Select(t => $"{t.Longitude.ValueAsText()} {t.Latitude.ValueAsText()}"))})",
                                        _ =>
                                            $"SRID=4326;LINESTRING({string.Join(",", intermediates.Select(t => $"{t.Longitude.ValueAsText()} {t.Latitude.ValueAsText()}"))})"
                                    },
                                    $"{string.Join(",", way.Tags.Select(t => $"\"{t.Key.TextEscape(2)}\"=>\"{t.Value.TextEscape(2)}\""))}",
                                    $"{string.Join(",", direction.Select(t => $"\"{t.Key.TextEscape(2)}\"=>\"{t.Value.ToString()}\""))}",
                                    $"{string.Join(",", weight.Select(t => $"\"{t.Key.TextEscape(2)}\"=>\"{t.Value.ValueAsText()}\""))}"
                                };

                                lock (objWriter)
                                {
                                    writer.WriteLine(string.Join("\t", values));
                                }
                            }
                        }

                        lock (objProgress)
                        {
                            if (current++ % 1000 == 0)
                                progressClient.Progress(100f * current / count, id, session).GetAwaiter()
                                    .GetResult();
                        }
                    }

                    osmConnection2.Close();
                    connection3.Close();
                });
            }

            await progressClient.Finalize(id, session);
        }


        id = Guid.NewGuid().ToString();
        await progressClient.Init(id, session);

        using (var command0 = new NpgsqlCommand(string.Join(";",
                   @"ANALYZE temp_edge"), connection))
        using (var command1 = new NpgsqlCommand(string.Join(";",
                   @"SELECT reltuples::bigint AS estimate FROM pg_class WHERE relname = 'temp_edge'",
                   @"CREATE INDEX ON temp_edge (guid,from_node,to_node,way)"), connection))
        using (var command2 = new NpgsqlCommand(string.Join(";",
                   @"INSERT INTO edge(
	                    guid,
	                    from_node,
	                    to_node,
	                    way,
	                    from_latitude, 
	                    from_longitude, 
	                    to_latitude, 
	                    to_longitude, 
	                    distance,
	                    coordinates,
	                    location,
	                    tags,
	                    direction,
	                    weight,
	                    nodes
                    ) WITH cte AS (
	                    SELECT *,ROW_NUMBER() OVER (PARTITION BY guid,from_node,to_node,way) AS rn 
                        FROM (SELECT * FROM temp_edge ORDER BY temp_id LIMIT @limit OFFSET @skip) q
                    ) SELECT 
	                    guid,
	                    from_node,
	                    to_node,
	                    way,
	                    from_latitude, 
	                    from_longitude, 
	                    to_latitude, 
	                    to_longitude, 
	                    distance,
	                    coordinates,
	                    location,
	                    tags,
	                    direction,
	                    weight,
	                    ARRAY[from_node,to_node]
                    FROM cte WHERE rn=1
                    ON CONFLICT (guid,from_node,to_node,way) DO NOTHING"), connection))
        using (var command3 = new NpgsqlCommand(string.Join(";",
                   @"DROP TABLE temp_edge"), connection))
        {
            command0.Prepare();
            command1.Prepare();
            command2.Parameters.Add("skip", NpgsqlDbType.Bigint);
            command2.Parameters.Add("limit", NpgsqlDbType.Bigint);
            command2.Prepare();
            command3.Prepare();

            await command0.ExecuteNonQueryAsync();

            var count = 0L;
            using (var reader = command1.ExecuteReader())
            {
                if (reader.Read())
                    count = reader.GetInt64(0);
            }

            var limit = 10000L;
            var current = 0L;
            for (var skip = 0L; skip < count; skip += limit)
            {
                command2.Parameters["skip"].Value = skip;
                command2.Parameters["limit"].Value = limit;
                command2.ExecuteNonQuery();
                current = Math.Min(count, skip + limit);
                await progressClient.Progress(100f * current / count, id, session);
            }

            command3.ExecuteNonQuery();
        }

        await progressClient.Finalize(id, session);

        id = Guid.NewGuid().ToString();
        await progressClient.Init(id, session);

        using (var command0 = new NpgsqlCommand(string.Join(";",
                       @"ANALYZE temp_restriction"),
                   connection2))
        using (var command1 = new NpgsqlCommand(string.Join(";",
                       @"SELECT reltuples::bigint AS estimate FROM pg_class WHERE relname = 'temp_restriction'"),
                   connection2))
        using (var command2 = new NpgsqlCommand(string.Join(";",
                   @"INSERT INTO temp_restriction2 (
	                            guid,
	                            vehicle_type,
	                            from_edge,
	                            to_edge,
	                            via_node,
	                            tags
                            ) SELECT
	                            t.guid,
	                            t.vehicle_type,
	                            ef.id AS from_edge,
	                            et.id AS to_edge,
	                            t.via_node,
	                            t.tags
                            FROM (SELECT * FROM temp_restriction ORDER BY temp_id LIMIT @limit OFFSET @skip) t JOIN edge ef ON t.from_way=ef.way JOIN edge et ON t.to_way=et.way
                            WHERE ef.guid=t.guid AND et.guid=t.guid
                            AND (ef.from_node=t.via_node OR ef.to_node=t.via_node)
                            AND (et.from_node=t.via_node OR et.to_node=t.via_node)"), connection2))
        using (var command3 = new NpgsqlCommand(string.Join(";",
                   @"ANALYZE temp_restriction2"), connection2))
        using (var command4 = new NpgsqlCommand(string.Join(";",
                   @"SELECT reltuples::bigint AS estimate FROM pg_class WHERE relname = 'temp_restriction2'",
                   @"CREATE INDEX ON temp_restriction2 (guid,vehicle_type,from_edge,to_edge,via_node)"), connection2))
        using (var command5 = new NpgsqlCommand(string.Join(";",
                   @"INSERT INTO restriction(
	                guid,
	                vehicle_type,
	                from_edge,
	                to_edge,
	                via_node,
	                tags
                ) WITH cte AS (
	                SELECT *,ROW_NUMBER() OVER (PARTITION BY guid,vehicle_type,from_edge,to_edge,via_node) AS rn 
                    FROM (SELECT * FROM temp_restriction2 ORDER BY temp_id LIMIT @limit OFFSET @skip) q
                ) SELECT
	                guid,
	                vehicle_type,
	                from_edge,
	                to_edge,
	                via_node,
	                tags
                FROM cte WHERE rn=1
                ON CONFLICT (guid,vehicle_type,from_edge,to_edge,via_node) DO NOTHING"), connection2))
        using (var command6 = new NpgsqlCommand(string.Join(";",
                   @"DROP TABLE temp_restriction2",
                   @"DROP TABLE temp_restriction"), connection2))
        {
            command0.Prepare();
            command1.Prepare();
            command2.Parameters.Add("skip", NpgsqlDbType.Bigint);
            command2.Parameters.Add("limit", NpgsqlDbType.Bigint);
            command2.Prepare();
            command3.Prepare();
            command4.Prepare();
            command5.Parameters.Add("skip", NpgsqlDbType.Bigint);
            command5.Parameters.Add("limit", NpgsqlDbType.Bigint);
            command5.Prepare();
            command6.Prepare();

            await command0.ExecuteNonQueryAsync();

            var count = 0L;
            using (var reader = command1.ExecuteReader())
            {
                if (reader.Read())
                    count = reader.GetInt64(0);
            }

            var limit = 10000L;
            var current = 0L;
            for (var skip = 0L; skip < count; skip += limit)
            {
                command2.Parameters["skip"].Value = skip;
                command2.Parameters["limit"].Value = limit;
                command2.ExecuteNonQuery();
                current = Math.Min(count, skip + limit);
                await progressClient.Progress(100f * current / count, id, session);
            }

            await progressClient.Finalize(id, session);
            id = Guid.NewGuid().ToString();
            await progressClient.Init(id, session);

            await command3.ExecuteNonQueryAsync();

            using (var reader = command4.ExecuteReader())
            {
                if (reader.Read())
                    count = reader.GetInt64(0);
            }

            current = 0L;
            for (var skip = 0L; skip < count; skip += limit)
            {
                command5.Parameters["skip"].Value = skip;
                command5.Parameters["limit"].Value = limit;
                command5.ExecuteNonQuery();
                current = Math.Min(count, skip + limit);
                await progressClient.Progress(100f * current / count, id, session);
            }

            command6.ExecuteNonQuery();
        }

        await progressClient.Finalize(id, session);

        await osmConnection.CloseAsync();
        await connection.CloseAsync();
        await connection2.CloseAsync();
    }

    private async Task ExecuteResourceAsync(Assembly assembly, string resource, NpgsqlConnection connection)
    {
        using var stream = assembly.GetManifestResourceStream(resource);
        using var sr = new StreamReader(stream, Encoding.UTF8);
        using var command = new NpgsqlCommand(await sr.ReadToEndAsync(), connection);
        command.Prepare();
        command.ExecuteNonQuery();
    }

    public class NodeItem
    {
        public long Id { get; set; }
        public float Latitude { get; set; }
        public float Longitude { get; set; }
    }
}