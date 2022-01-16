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
using Placium.Route.Profiles;
using Placium.Route.Restructions;
using Placium.Types;
using Route.LocalGeo;
using Route.Profiles.Lua.DataTypes;

namespace Placium.Route
{
    public class RouterDb
    {
        private readonly HashSet<string> _vehicleTypes;

        public RouterDb(Guid guid, string connectionString,
            Vehicle[] vehicles)
        {
            Guid = guid;
            ConnectionString = connectionString;
            VehicleCache = new VehicleCache(vehicles);
            _vehicleTypes = new HashSet<string>();

            foreach (var vehicle in VehicleCache.Vehicles)
            foreach (var vehicleType in vehicle.VehicleTypes)
                _vehicleTypes.Add(vehicleType);
        }

        public VehicleCache VehicleCache { get; }
        public Guid Guid { get; }
        public string ConnectionString { get; }

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

                var id = Guid.NewGuid().ToString();
                await progressClient.Init(id, session);
                using (var writer = connection.BeginTextImport(
                    "COPY temp_node (guid,id,latitude,longitude,tags,is_core) FROM STDIN WITH NULL AS ''"))
                {
                    using (var command = new NpgsqlCommand(string.Join(";", @"SELECT COUNT(*) FROM way", @"SELECT
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
                        command.Prepare();

                        using var reader = await command.ExecuteReaderAsync();
                        if (!reader.Read()) throw new NullReferenceException();
                        var count = reader.GetInt64(0);
                        await reader.NextResultAsync();
                        var current = 0L;
                        var objReader = new object();
                        var objFoundRestriction = new object();
                        var objWriter = new object();
                        var objProgress = new object();
                        var objVehicleCache = new object();
                        var doIt = true;

                        Parallel.For(0, 8, i =>
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

                                var attributes = way.Tags.ToAttributes();
                                bool anyCanTraverse;
                                lock (objVehicleCache)
                                {
                                    anyCanTraverse = VehicleCache.AnyCanTraverse(attributes);
                                }

                                if (anyCanTraverse)
                                {
                                    command2.Parameters["ids"].Value = way.Nodes;

                                    using var reader2 = command2.ExecuteReader();
                                    while (reader2.Read())
                                    {
                                        var node = new Node().Fill(reader2);

                                        var is_core = node.Id == way.Nodes.First() || node.Id == way.Nodes.Last();

                                        var nodeTags = node.Tags;
                                        if (nodeTags != null &&
                                            (nodeTags.Contains("barrier", "bollard") ||
                                             nodeTags.Contains("barrier", "fence") ||
                                             nodeTags.Contains("barrier", "gate")))
                                        {
                                            is_core = true;
                                            lock (objFoundRestriction)
                                            {
                                                FoundRestriction("motorcar", way.Id.Value, way.Id.Value, node.Id.Value,
                                                    nodeTags);
                                            }
                                        }

                                        lock (objVehicleCache)
                                        {
                                            foreach (var vehicle in VehicleCache.Vehicles)
                                                if (vehicle is DynamicVehicle dynamicVehicle)
                                                {
                                                    var nodeRestrictionFunc =
                                                        dynamicVehicle.Script.Globals["node_restriction"];

                                                    if (nodeRestrictionFunc == null) continue;

                                                    var attributesTable = new Table(dynamicVehicle.Script);
                                                    var resultsTable = new Table(dynamicVehicle.Script);

                                                    lock (dynamicVehicle.Script)
                                                    {
                                                        // build lua table.
                                                        attributesTable.Clear();
                                                        foreach (var attribute in nodeTags)
                                                            attributesTable.Set(attribute.Key,
                                                                DynValue.NewString(attribute.Value));

                                                        // call factor_and_speed function.
                                                        resultsTable.Clear();
                                                        dynamicVehicle.Script.Call(nodeRestrictionFunc, attributesTable,
                                                            resultsTable);

                                                        // get the vehicle type if any.
                                                        var vehicleTypeVal = resultsTable.Get("vehicle");
                                                        if (vehicleTypeVal != null &&
                                                            vehicleTypeVal.Type == DataType.String)
                                                        {
                                                            // restriction found.
                                                            is_core = true;

                                                            var vehicleType = vehicleTypeVal.String;
                                                            lock (objFoundRestriction)
                                                            {
                                                                FoundRestriction(vehicleType, way.Id.Value,
                                                                    way.Id.Value,
                                                                    node.Id.Value,
                                                                    nodeTags);
                                                            }
                                                        }
                                                    }
                                                }
                                        }

                                        if (!is_core && nodeTags != null && nodeTags.Any())
                                            lock (objVehicleCache)
                                            {
                                                foreach (var vehicle in VehicleCache.Vehicles)
                                                    if (vehicle is DynamicVehicle dynamicVehicle)
                                                    {
                                                        var nodeTagProcessor =
                                                            dynamicVehicle.Script.Globals["node_tag_processor"];

                                                        if (nodeTagProcessor == null) continue;

                                                        var attributesTable = new Table(dynamicVehicle.Script);
                                                        var resultsTable = new Table(dynamicVehicle.Script);

                                                        lock (dynamicVehicle.Script)
                                                        {
                                                            // build lua table.
                                                            attributesTable.Clear();
                                                            foreach (var attribute in nodeTags)
                                                                attributesTable.Set(attribute.Key,
                                                                    DynValue.NewString(attribute.Value));

                                                            // call factor_and_speed function.
                                                            resultsTable.Clear();
                                                            dynamicVehicle.Script.Call(nodeTagProcessor,
                                                                attributesTable,
                                                                resultsTable);

                                                            // get the result.
                                                            var dynAttributesToKeep =
                                                                resultsTable.Get("attributes_to_keep");
                                                            if (dynAttributesToKeep != null &&
                                                                dynAttributesToKeep.Type != DataType.Nil &&
                                                                dynAttributesToKeep.Table.Keys.Any())
                                                            {
                                                                is_core = true;
                                                                break;
                                                            }
                                                        }
                                                    }
                                            }

                                        var values = new[]
                                        {
                                            Guid.ToString(),
                                            node.Id.ToString(),
                                            node.Latitude.ValueAsText(),
                                            node.Longitude.ValueAsText(),
                                            $"{string.Join(",", node.Tags.Select(t => $"\"{t.Key.TextEscape(2)}\"=>\"{t.Value.TextEscape(2)}\""))}",
                                            is_core.ValueAsText()
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
                        });
                    }
                }

                await ExecuteResourceAsync(Assembly.GetExecutingAssembly(),
                    "Placium.Route.InsertFromTempTables.pgsql",
                    connection);
                await progressClient.Finalize(id, session);

                id = Guid.NewGuid().ToString();
                await progressClient.Init(id, session);

                using (var command = new NpgsqlCommand(string.Join(";", @"SELECT COUNT(*) 
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
                        var via = (long[]) reader.GetValue(9);

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
                    var objVehicleCache = new object();
                    var doIt = true;

                    Parallel.For(0, 8, i =>
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

                            var attributes1 = way1.Tags.ToAttributes();
                            var attributes2 = way2.Tags.ToAttributes();
                            bool anyCanTraverse;
                            lock (objVehicleCache)
                            {
                                anyCanTraverse = way1.Nodes.Length > 1 && way2.Nodes.Length > 1 &&
                                                 VehicleCache.AnyCanTraverse(attributes1) &&
                                                 VehicleCache.AnyCanTraverse(attributes2);
                            }

                            if (anyCanTraverse)
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
                                    x => new Coordinate((float) x.Latitude, (float) x.Longitude));
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
                using (var command = new NpgsqlCommand(string.Join(";", @"SELECT COUNT(*) FROM way", @"SELECT
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
                    command.Prepare();

                    using var reader = await command.ExecuteReaderAsync();
                    if (!reader.Read()) throw new NullReferenceException();
                    var count = reader.GetInt64(0);
                    await reader.NextResultAsync();
                    var current = 0L;
                    var objReader = new object();
                    var objWriter = new object();
                    var objProgress = new object();
                    var objVehicleCache = new object();
                    var doIt = true;

                    Parallel.For(0, 8, j =>
                    {
                        using var connection3 = new NpgsqlConnection(ConnectionString);
                        connection3.Open();

                        using var command3 = new NpgsqlCommand(
                            @"SELECT id,latitude,longitude,is_core FROM node WHERE id=ANY(@ids) AND guid=@guid",
                            connection3);

                        command3.Parameters.Add("ids", NpgsqlDbType.Array | NpgsqlDbType.Bigint);
                        command3.Parameters.AddWithValue("guid", Guid);
                        command3.Prepare();

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

                            var attributes = way.Tags.ToAttributes();
                            bool anyCanTraverse;
                            lock (objVehicleCache)
                            {
                                anyCanTraverse = VehicleCache.AnyCanTraverse(attributes);
                            }

                            if (anyCanTraverse)
                            {
                                var factorAndSpeeds = new Dictionary<string, FactorAndSpeed>();
                                foreach (var vehicle in VehicleCache.Vehicles)
                                foreach (var profile in vehicle.GetProfiles())
                                    factorAndSpeeds.Add(profile.FullName, profile.FactorAndSpeed(attributes));

                                command3.Parameters["ids"].Value = way.Nodes;

                                var list = new List<NodeItem>(way.Nodes.Length);

                                using (var reader3 = command3.ExecuteReader())
                                {
                                    while (reader3.Read())
                                        list.Add(new NodeItem
                                        {
                                            Id = reader3.GetInt64(0),
                                            Latitude = reader3.GetFloat(1),
                                            Longitude = reader3.GetFloat(2),
                                            IsCore = reader3.GetBoolean(3)
                                        });
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

                                    var toNode = (long?) null;
                                    while (true)
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

                                        if (item.IsCore)
                                        {
                                            // node is part of the core.
                                            toNode = way.Nodes[i];
                                            break;
                                        }

                                        i++;
                                    }

                                    if (toNode == null) break;

                                    var direction = factorAndSpeeds.ToDictionary(x => x.Key, x => x.Value.Direction);
                                    var weight = factorAndSpeeds.Where(x => x.Value.Value > 0)
                                        .ToDictionary(x => x.Key, x => distance * x.Value.Value);

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
                                        intermediates.Count switch {
                                            0=>"SRID=4326;POINT EMPTY",
                                            1=>
                                            $"SRID=4326;POINT({string.Join(",", intermediates.Select(t => $"{t.Longitude.ValueAsText()} {t.Latitude.ValueAsText()}"))})"
                                            ,
                                            _=>
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

                        connection3.Close();
                    });
                }

                await ExecuteResourceAsync(Assembly.GetExecutingAssembly(),
                    "Placium.Route.InsertFromTempTables3.pgsql",
                    connection);
                await progressClient.Finalize(id, session);
            }

            await ExecuteResourceAsync(Assembly.GetExecutingAssembly(),
                "Placium.Route.InsertFromTempTables2.pgsql",
                connection2);


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
            public bool IsCore { get; set; }
        }
    }
}