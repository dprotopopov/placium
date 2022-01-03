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
            using (var osmConnection = new NpgsqlConnection(osmConnectionString))
            using (var osmConnection2 = new NpgsqlConnection(osmConnectionString))
            using (var connection = new NpgsqlConnection(ConnectionString))
            using (var connection2 = new NpgsqlConnection(ConnectionString))
            using (var connection3 = new NpgsqlConnection(ConnectionString))
            {
                await osmConnection.OpenAsync();
                await osmConnection2.OpenAsync();
                await connection.OpenAsync();
                await connection2.OpenAsync();
                await connection3.OpenAsync();

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
                    "COPY temp_restriction (guid,vehicle_type,from_nodes,to_nodes,via_nodes,tags) FROM STDIN WITH NULL AS ''")
                )
                {
                    void FoundRestriction(string vehicleType, long[] from, long[] to, long[] via,
                        TagsCollectionBase tags)
                    {
                        if (vehicleType == null) vehicleType = string.Empty;

                        var values = new[]
                        {
                            Guid.ToString(),
                            vehicleType,
                            $"{{{string.Join(",", from.Select(t => $"{t}"))}}}",
                            $"{{{string.Join(",", to.Select(t => $"{t}"))}}}",
                            $"{{{string.Join(",", via.Select(t => $"{t}"))}}}",
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
                        using (var command2 = new NpgsqlCommand(@"SELECT 
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
                        FROM node WHERE id=ANY(@ids)", osmConnection2))
                        {
                            command2.Parameters.Add("ids", NpgsqlDbType.Array | NpgsqlDbType.Bigint);
                            command2.Prepare();
                            command.Prepare();

                            using var reader = await command.ExecuteReaderAsync();
                            if (!reader.Read()) throw new NullReferenceException();
                            var count = reader.GetInt64(0);
                            var current = 0L;
                            await reader.NextResultAsync();
                            while (reader.Read())
                            {
                                var way = new Way().Fill(reader);

                                var attributes = way.Tags.ToAttributes();
                                if (VehicleCache.AnyCanTraverse(attributes))
                                {
                                    command2.Parameters["ids"].Value = way.Nodes;

                                    using var reader2 = await command2.ExecuteReaderAsync();
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
                                            var via = new[] {node.Id.Value};
                                            FoundRestriction("motorcar", way.Nodes, way.Nodes, via, nodeTags);
                                        }

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
                                                        var via = new[] {node.Id.Value};
                                                        FoundRestriction(vehicleType, way.Nodes, way.Nodes, via,
                                                            nodeTags);
                                                    }
                                                }
                                            }

                                        if (!is_core && nodeTags != null && nodeTags.Any())
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
                                                        dynamicVehicle.Script.Call(nodeTagProcessor, attributesTable,
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

                                        var values = new[]
                                        {
                                            Guid.ToString(),
                                            node.Id.ToString(),
                                            node.Latitude.ValueAsText(),
                                            node.Longitude.ValueAsText(),
                                            $"{string.Join(",", node.Tags.Select(t => $"\"{t.Key.TextEscape(2)}\"=>\"{t.Value.TextEscape(2)}\""))}",
                                            is_core.ValueAsText()
                                        };

                                        writer.WriteLine(string.Join("\t", values));
                                    }
                                }

                                if (current++ % 1000 == 0)
                                    await progressClient.Progress(100f * current / count, id, session);
                            }
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
                    using (var command2 = new NpgsqlCommand(string.Join(";", @"SELECT
                            id,
                            version,
                            change_set_id,
                            time_stamp,
                            user_id,
                            user_name,
                            visible,
                            tags,
                            nodes
                        FROM way WHERE nodes&&@nodes"), osmConnection2))
                    {
                        command2.Parameters.Add("nodes", NpgsqlDbType.Array | NpgsqlDbType.Bigint);
                        command2.Prepare();
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

                            if (relation.IsRestriction(out var vehicleType) &&
                                relation.Members != null)
                            {
                                if (string.IsNullOrWhiteSpace(vehicleType)) vehicleType = "motorcar";

                                var type = "restriction";
                                if (!string.IsNullOrWhiteSpace(vehicleType)) type = type + ":" + vehicleType;

                                long? from = null;
                                long? to = null;
                                foreach (var member in relation.Members)
                                    switch (member.Role)
                                    {
                                        case "from":
                                            from = member.Id;
                                            break;
                                        case "to":
                                            to = member.Id;
                                            break;
                                    }

                                command2.Parameters["nodes"].Value = via;

                                using var reader2 = await command2.ExecuteReaderAsync();

                                var list = new List<Way>();
                                while (reader2.Read())
                                {
                                    var way = new Way().Fill(reader2);
                                    list.Add(way);
                                }

                                var fromWay = list.FirstOrDefault(x => x.Id == from);
                                if (fromWay != null)
                                    foreach (var way in list.Where(x => x.Id != from && x.Id != to))
                                        FoundRestriction(vehicleType, fromWay.Nodes, way.Nodes, via,
                                            new TagsCollection(
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
                    using (var command2 = new NpgsqlCommand(string.Join(";", @"SELECT
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
                        FROM node WHERE id=ANY(@ids)"), osmConnection2))
                    {
                        command2.Parameters.Add("ids", NpgsqlDbType.Array | NpgsqlDbType.Bigint);
                        command2.Prepare();
                        command.Prepare();

                        var vehicleType = string.Empty;
                        if (string.IsNullOrWhiteSpace(vehicleType)) vehicleType = "motorcar";

                        var type = "restriction";
                        if (!string.IsNullOrWhiteSpace(vehicleType)) type = type + ":" + vehicleType;

                        using var reader = await command.ExecuteReaderAsync();
                        if (!reader.Read()) throw new NullReferenceException();
                        var count = reader.GetInt64(0);
                        var current = 0L;
                        await reader.NextResultAsync();
                        while (reader.Read())
                        {
                            var way1 = new Way().Fill(reader);
                            var way2 = new Way().Fill(reader, 9);

                            var attributes1 = way1.Tags.ToAttributes();
                            var attributes2 = way2.Tags.ToAttributes();
                            if (way1.Nodes.Length > 1 && way2.Nodes.Length > 1 &&
                                VehicleCache.AnyCanTraverse(attributes1) &&
                                VehicleCache.AnyCanTraverse(attributes2))
                            {
                                var intersection =
                                    (from n1 in way1.Nodes join n2 in way2.Nodes on n1 equals n2 select n1)
                                    .ToList();

                                var ids = new List<long>(way1.Nodes.Length + way2.Nodes.Length);
                                ids.AddRange(way1.Nodes);
                                ids.AddRange(way2.Nodes);

                                command2.Parameters["ids"].Value = ids.ToArray();

                                var list = new List<Node>(way1.Nodes.Length + way2.Nodes.Length - intersection.Count);
                                using var reader2 = command2.ExecuteReader();

                                while (reader2.Read())
                                {
                                    var node = new Node().Fill(reader2);
                                    list.Add(node);
                                }

                                var dictionary = list.ToDictionary(x => x.Id,
                                    x => new Coordinate((float) x.Latitude, (float) x.Longitude));

                                foreach (var via in intersection)
                                {
                                    var index1 = way1.Nodes.TakeWhile(node => node != via).Count();
                                    var index2 = way2.Nodes.TakeWhile(node => node != via).Count();

                                    if (index1 > 0 && index2 > 0)
                                    {
                                        var from = way1.Nodes[index1 - 1];
                                        var to = way2.Nodes[index2 - 1];

                                        var fromCoords = dictionary[from];
                                        var toCoords = dictionary[to];
                                        var viaCoords = dictionary[via];

                                        if (Coordinate.AngleInDegree(fromCoords, toCoords, viaCoords) < 60f)
                                        {
                                            FoundRestriction(vehicleType, way1.Nodes.Take(index1).ToArray(),
                                                way2.Nodes.Take(index2).ToArray(), new[] {via},
                                                new TagsCollection(
                                                    new Tag("type", type),
                                                    new Tag("restriction", "no_turn")));
                                            FoundRestriction(vehicleType, way2.Nodes.Take(index2).ToArray(),
                                                way1.Nodes.Take(index1).ToArray(), new[] {via},
                                                new TagsCollection(
                                                    new Tag("type", type),
                                                    new Tag("restriction", "no_turn")));
                                        }
                                    }

                                    if (index1 > 0 && index2 < way2.Nodes.Length - 1)
                                    {
                                        var from = way1.Nodes[index1 - 1];
                                        var to = way2.Nodes[index2 + 1];

                                        var fromCoords = dictionary[from];
                                        var toCoords = dictionary[to];
                                        var viaCoords = dictionary[via];

                                        if (Coordinate.AngleInDegree(fromCoords, toCoords, viaCoords) < 60f)
                                        {
                                            FoundRestriction(vehicleType, way1.Nodes.Take(index1).ToArray(),
                                                way2.Nodes.Skip(index2 + 1).ToArray(), new[] {via},
                                                new TagsCollection(
                                                    new Tag("type", type),
                                                    new Tag("restriction", "no_turn")));
                                            FoundRestriction(vehicleType, way2.Nodes.Skip(index2 + 1).ToArray(),
                                                way1.Nodes.Take(index1).ToArray(), new[] {via},
                                                new TagsCollection(
                                                    new Tag("type", type),
                                                    new Tag("restriction", "no_turn")));
                                        }
                                    }

                                    if (index1 < way1.Nodes.Length - 1 && index2 < way2.Nodes.Length - 1)
                                    {
                                        var from = way1.Nodes[index1 + 1];
                                        var to = way2.Nodes[index2 + 1];

                                        var fromCoords = dictionary[from];
                                        var toCoords = dictionary[to];
                                        var viaCoords = dictionary[via];

                                        if (Coordinate.AngleInDegree(fromCoords, toCoords, viaCoords) < 60f)
                                        {
                                            FoundRestriction(vehicleType, way1.Nodes.Skip(index1 + 1).ToArray(),
                                                way2.Nodes.Skip(index2 + 1).ToArray(), new[] {via},
                                                new TagsCollection(
                                                    new Tag("type", type),
                                                    new Tag("restriction", "no_turn")));
                                            FoundRestriction(vehicleType, way2.Nodes.Skip(index2 + 1).ToArray(),
                                                way1.Nodes.Skip(index1 + 1).ToArray(), new[] {via},
                                                new TagsCollection(
                                                    new Tag("type", type),
                                                    new Tag("restriction", "no_turn")));
                                        }
                                    }

                                    if (index1 < way1.Nodes.Length - 1 && index2 > 0)
                                    {
                                        var from = way1.Nodes[index1 + 1];
                                        var to = way2.Nodes[index2 - 1];

                                        var fromCoords = dictionary[from];
                                        var toCoords = dictionary[to];
                                        var viaCoords = dictionary[via];

                                        if (Coordinate.AngleInDegree(fromCoords, toCoords, viaCoords) < 60f)
                                        {
                                            FoundRestriction(vehicleType, way1.Nodes.Skip(index1 + 1).ToArray(),
                                                way2.Nodes.Take(index2).ToArray(), new[] {via},
                                                new TagsCollection(
                                                    new Tag("type", type),
                                                    new Tag("restriction", "no_turn")));
                                            FoundRestriction(vehicleType, way2.Nodes.Take(index2).ToArray(),
                                                way1.Nodes.Skip(index1 + 1).ToArray(), new[] {via},
                                                new TagsCollection(
                                                    new Tag("type", type),
                                                    new Tag("restriction", "no_turn")));
                                        }
                                    }
                                }
                            }

                            if (current++ % 1000 == 0)
                                await progressClient.Progress(100f * current / count, id, session);
                        }
                    }

                    await progressClient.Finalize(id, session);

                    id = Guid.NewGuid().ToString();
                    await progressClient.Init(id, session);

                    using (var writer = connection.BeginTextImport(
                        @"COPY temp_edge (guid,from_node,to_node,
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
                    using (var command3 = new NpgsqlCommand(
                        @"SELECT id,latitude,longitude,is_core FROM node WHERE id=ANY(@ids) AND guid=@guid",
                        connection3))
                    {
                        command3.Parameters.Add("ids", NpgsqlDbType.Array | NpgsqlDbType.Bigint);
                        command3.Parameters.AddWithValue("guid", Guid);
                        command3.Prepare();
                        command.Prepare();

                        using var reader = await command.ExecuteReaderAsync();
                        if (!reader.Read()) throw new NullReferenceException();
                        var count = reader.GetInt64(0);
                        var current = 0L;
                        await reader.NextResultAsync();
                        while (reader.Read())
                        {
                            var way = new Way().Fill(reader);

                            var attributes = way.Tags.ToAttributes();
                            if (VehicleCache.AnyCanTraverse(attributes))
                            {
                                var factorAndSpeeds = new Dictionary<string, FactorAndSpeed>();
                                foreach (var vehicle in VehicleCache.Vehicles)
                                foreach (var profile in vehicle.GetProfiles())
                                    factorAndSpeeds.Add(profile.FullName, profile.FactorAndSpeed(attributes));

                                command3.Parameters["ids"].Value = way.Nodes;

                                var list = new List<NodeItem>(way.Nodes.Length);

                                using (var reader3 = await command3.ExecuteReaderAsync())
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
                                        .ToDictionary(x => x.Key, x => distance / x.Value.Value);

                                    var fromCoords = intermediates.First();
                                    var toCoords = intermediates.Last();

                                    var values = new[]
                                    {
                                        Guid.ToString(),
                                        fromNode.ToString(),
                                        toNode.ToString(),
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

                                    writer.WriteLine(string.Join("\t", values));
                                }
                            }

                            if (current++ % 1000 == 0)
                                await progressClient.Progress(100f * current / count, id, session);
                        }
                    }

                    await ExecuteResourceAsync(Assembly.GetExecutingAssembly(),
                        "Placium.Route.InsertFromTempTables3.pgsql",
                        connection);
                    await progressClient.Finalize(id, session);
                }

                await ExecuteResourceAsync(Assembly.GetExecutingAssembly(),
                    "Placium.Route.InsertFromTempTables2.pgsql",
                    connection2);

                #region region

#if _REGION
                await ExecuteResourceAsync(Assembly.GetExecutingAssembly(),
                    "Placium.Route.CreateTempTables4.pgsql",
                    connection);
                using (var command =
                    new NpgsqlCommand(
                        @"INSERT INTO region(guid,min_latitude,min_longitude,max_latitude,max_longitude)
                                VALUES (@guid,-90,-180,90,180)",
                        connection))
                {
                    command.Parameters.AddWithValue("guid", Guid);
                    command.Prepare();
                    command.ExecuteNonQuery();
                }

                var id1 = Guid.NewGuid().ToString();
                await progressClient.Init(id1, session);

                long totalNodes;
                using (var command =
                    new NpgsqlCommand(
                        @"SELECT COUNT(*) FROM node WHERE guid=@guid",
                        connection))
                {
                    command.Parameters.AddWithValue("guid", Guid);
                    command.Prepare();
                    totalNodes = (long) await command.ExecuteScalarAsync();
                }

                var totalUnits = 1L << (int) Math.Ceiling(Math.Log2(totalNodes) / 3);

                using (var command =
                    new NpgsqlCommand(
                        @"SELECT id,min_latitude,min_longitude,max_latitude,max_longitude FROM region WHERE guid=@guid",
                        connection))
                using (var command1 =
                    new NpgsqlCommand(
                        @"SELECT (PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY latitude))::real FROM node 
                                WHERE @minLatitude<=latitude AND latitude<=@maxLatitude
                                AND @minLongitude<=longitude AND longitude<=@maxLongitude
                                AND guid=@guid",
                        connection2))
                using (var command2 =
                    new NpgsqlCommand(
                        @"SELECT (PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY longitude))::real FROM node 
                                WHERE @minLatitude<=latitude AND latitude<=@maxLatitude
                                AND @minLongitude<=longitude AND longitude<=@maxLongitude
                                AND guid=@guid",
                        connection2))
                using (var command3 =
                    new NpgsqlCommand(string.Join(";",
                            @"INSERT INTO region(guid,min_latitude,min_longitude,max_latitude,max_longitude)
                                VALUES (@guid,@minLatitude1,@minLongitude1,@maxLatitude1,@maxLongitude1),
                                (@guid,@minLatitude2,@minLongitude2,@maxLatitude2,@maxLongitude2)",
                            @"DELETE FROM region WHERE id=@id AND guid=@guid"),
                        connection2))
                {
                    command.Parameters.AddWithValue("guid", Guid);
                    command.Prepare();

                    command1.Parameters.Add("minLatitude", NpgsqlDbType.Real);
                    command1.Parameters.Add("maxLatitude", NpgsqlDbType.Real);
                    command1.Parameters.Add("minLongitude", NpgsqlDbType.Real);
                    command1.Parameters.Add("maxLongitude", NpgsqlDbType.Real);
                    command1.Parameters.AddWithValue("guid", Guid);
                    command1.Prepare();

                    command2.Parameters.Add("minLatitude", NpgsqlDbType.Real);
                    command2.Parameters.Add("maxLatitude", NpgsqlDbType.Real);
                    command2.Parameters.Add("minLongitude", NpgsqlDbType.Real);
                    command2.Parameters.Add("maxLongitude", NpgsqlDbType.Real);
                    command2.Parameters.AddWithValue("guid", Guid);
                    command2.Prepare();

                    command3.Parameters.Add("id", NpgsqlDbType.Bigint);
                    command3.Parameters.Add("minLatitude1", NpgsqlDbType.Real);
                    command3.Parameters.Add("maxLatitude1", NpgsqlDbType.Real);
                    command3.Parameters.Add("minLongitude1", NpgsqlDbType.Real);
                    command3.Parameters.Add("maxLongitude1", NpgsqlDbType.Real);
                    command3.Parameters.Add("minLatitude2", NpgsqlDbType.Real);
                    command3.Parameters.Add("maxLatitude2", NpgsqlDbType.Real);
                    command3.Parameters.Add("minLongitude2", NpgsqlDbType.Real);
                    command3.Parameters.Add("maxLongitude2", NpgsqlDbType.Real);
                    command3.Parameters.AddWithValue("guid", Guid);
                    command3.Prepare();

                    var current = 0L;

                    for (var i = totalUnits; i > 1; i >>= 1)
                        using (var reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                long uid;
                                float minLatitude;
                                float minLongitude;
                                float maxLatitude;
                                float maxLongitude;


                                uid = reader.GetInt64(0);
                                minLatitude = reader.GetFloat(1);
                                minLongitude = reader.GetFloat(2);
                                maxLatitude = reader.GetFloat(3);
                                maxLongitude = reader.GetFloat(4);


                                if (maxLatitude - minLatitude > maxLongitude - minLongitude)
                                {
                                    command1.Parameters["minLatitude"].Value = minLatitude;
                                    command1.Parameters["minLongitude"].Value = minLongitude;
                                    command1.Parameters["maxLatitude"].Value = maxLatitude;
                                    command1.Parameters["maxLongitude"].Value = maxLongitude;

                                    var latitude = (float) command1.ExecuteScalar();

                                    command3.Parameters["id"].Value = uid;
                                    command3.Parameters["minLatitude1"].Value = minLatitude;
                                    command3.Parameters["minLongitude1"].Value = minLongitude;
                                    command3.Parameters["maxLatitude1"].Value = latitude;
                                    command3.Parameters["maxLongitude1"].Value = maxLongitude;
                                    command3.Parameters["minLatitude2"].Value = latitude;
                                    command3.Parameters["minLongitude2"].Value = minLongitude;
                                    command3.Parameters["maxLatitude2"].Value = maxLatitude;
                                    command3.Parameters["maxLongitude2"].Value = maxLongitude;

                                    command3.ExecuteNonQuery();
                                }
                                else
                                {
                                    command2.Parameters["minLatitude"].Value = minLatitude;
                                    command2.Parameters["minLongitude"].Value = minLongitude;
                                    command2.Parameters["maxLatitude"].Value = maxLatitude;
                                    command2.Parameters["maxLongitude"].Value = maxLongitude;

                                    var longitude = (float) command2.ExecuteScalar();

                                    command3.Parameters["id"].Value = uid;
                                    command3.Parameters["minLatitude1"].Value = minLatitude;
                                    command3.Parameters["minLongitude1"].Value = minLongitude;
                                    command3.Parameters["maxLatitude1"].Value = maxLatitude;
                                    command3.Parameters["maxLongitude1"].Value = longitude;
                                    command3.Parameters["minLatitude2"].Value = minLatitude;
                                    command3.Parameters["minLongitude2"].Value = longitude;
                                    command3.Parameters["maxLatitude2"].Value = maxLatitude;
                                    command3.Parameters["maxLongitude2"].Value = maxLongitude;

                                    command3.ExecuteNonQuery();
                                }

                                await progressClient.Progress(100f * ++current / totalUnits, id1, session);
                            }
                        }
                }

                await progressClient.Finalize(id1, session);

                using (var command =
                    new NpgsqlCommand(string.Join(";",
                            @"SELECT COUNT(*) FROM region u1,region u2
                                WHERE u1.guid=@guid AND u2.guid=@guid AND u1.id<>u2.id",
                            @"SELECT u1.id,u1.min_latitude,u1.min_longitude,u1.max_latitude,u1.max_longitude,
                                    u2.id,u2.min_latitude,u2.min_longitude,u2.max_latitude,u2.max_longitude
                                FROM region u1,region u2
                                WHERE u1.guid=@guid AND u2.guid=@guid AND u1.id<>u2.id"),
                        connection2))
                using (var command2 =
                    new NpgsqlCommand(
                        @"SELECT array_agg(q.id) FROM (SELECT DISTINCT id FROM edge,unnest(avals(direction)) v                                                                                        
                                WHERE (v::smallint=ANY(ARRAY[0,1,3,4])
                                AND @minLatitude1<=from_latitude AND from_latitude<=@maxLatitude1
                                AND @minLongitude1<=from_longitude AND from_longitude<=@maxLongitude1
                                AND @minLatitude2<=to_latitude AND to_latitude<=@maxLatitude2
                                AND @minLongitude2<=to_longitude AND to_longitude<=@maxLongitude2
                                OR v::smallint=ANY(ARRAY[0,2,3,5])
                                AND @minLatitude1<=to_latitude AND to_latitude<=@maxLatitude1
                                AND @minLongitude1<=to_longitude AND to_longitude<=@maxLongitude1
                                AND @minLatitude2<=from_latitude AND from_latitude<=@maxLatitude2
                                AND @minLongitude2<=from_longitude AND from_longitude<=@maxLongitude2)
                                AND guid=@guid) q",
                        connection))
                using (var command3 =
                    new NpgsqlCommand(@"SELECT hstore(array_agg(q.k),array_agg(q.agg_v)) 
                                FROM (SELECT k,MIN(v::real)::text AS agg_v 
                                FROM edge,unnest(akeys(weight)) k,unnest(avals(weight)) v                                                                                        
                                WHERE id=ANY(@edges) AND guid=@guid GROUP BY k) q",
                        connection))
                using (var command4 =
                    new NpgsqlCommand(
                        @"INSERT INTO region_edge(guid,from_region,to_region,edges,weight)
                                VALUES (@guid,@fromRegion,@toRegion,@edges,@weight)",
                        connection))
                {
                    command.Parameters.AddWithValue("guid", Guid);
                    command.Prepare();

                    command2.Parameters.Add("minLatitude1", NpgsqlDbType.Real);
                    command2.Parameters.Add("maxLatitude1", NpgsqlDbType.Real);
                    command2.Parameters.Add("minLongitude1", NpgsqlDbType.Real);
                    command2.Parameters.Add("maxLongitude1", NpgsqlDbType.Real);
                    command2.Parameters.Add("minLatitude2", NpgsqlDbType.Real);
                    command2.Parameters.Add("maxLatitude2", NpgsqlDbType.Real);
                    command2.Parameters.Add("minLongitude2", NpgsqlDbType.Real);
                    command2.Parameters.Add("maxLongitude2", NpgsqlDbType.Real);
                    command2.Parameters.AddWithValue("guid", Guid);
                    command2.Prepare();

                    command3.Parameters.Add("edges", NpgsqlDbType.Array | NpgsqlDbType.Bigint);
                    command3.Parameters.AddWithValue("guid", Guid);
                    command3.Prepare();

                    command4.Parameters.Add("fromRegion", NpgsqlDbType.Bigint);
                    command4.Parameters.Add("toRegion", NpgsqlDbType.Bigint);
                    command4.Parameters.Add("edges", NpgsqlDbType.Array | NpgsqlDbType.Bigint);
                    command4.Parameters.Add("weight", NpgsqlDbType.Hstore);
                    command4.Parameters.AddWithValue("guid", Guid);
                    command4.Prepare();

                    var id = Guid.NewGuid().ToString();
                    await progressClient.Init(id, session);

                    var current = 0;

                    using var reader = await command.ExecuteReaderAsync();
                    if (!reader.Read()) throw new NullReferenceException();
                    var count = reader.GetInt64(0);
                    reader.NextResult();
                    while (reader.Read())
                    {
                        var uid1 = reader.GetInt64(0);
                        var minLatitude1 = reader.GetFloat(1);
                        var minLongitude1 = reader.GetFloat(2);
                        var maxLatitude1 = reader.GetFloat(3);
                        var maxLongitude1 = reader.GetFloat(4);

                        var uid2 = reader.GetInt64(5);
                        var minLatitude2 = reader.GetFloat(6);
                        var minLongitude2 = reader.GetFloat(7);
                        var maxLatitude2 = reader.GetFloat(8);
                        var maxLongitude2 = reader.GetFloat(9);

                        command2.Parameters["minLatitude1"].Value = minLatitude1;
                        command2.Parameters["minLongitude1"].Value = minLongitude1;
                        command2.Parameters["maxLatitude1"].Value = maxLatitude1;
                        command2.Parameters["maxLongitude1"].Value = maxLongitude1;
                        command2.Parameters["minLatitude2"].Value = minLatitude2;
                        command2.Parameters["minLongitude2"].Value = minLongitude2;
                        command2.Parameters["maxLatitude2"].Value = maxLatitude2;
                        command2.Parameters["maxLongitude2"].Value = maxLongitude2;

                        if (command2.ExecuteScalar() is long[] edges && edges.Any())
                        {
                            command3.Parameters["edges"].Value = edges;

                            if (command3.ExecuteScalar() is Dictionary<string, string> weight && weight.Any())
                            {
                                command4.Parameters["fromRegion"].Value = uid1;
                                command4.Parameters["toRegion"].Value = uid2;
                                command4.Parameters["edges"].Value = edges;
                                command4.Parameters["weight"].Value = weight;

                                command4.ExecuteNonQuery();
                            }
                        }

                        await progressClient.Progress(100f * ++current / count, id, session);
                    }

                    await progressClient.Finalize(id, session);
                }
#endif

#endregion


#region restriction

                using (var command7 =
                    new NpgsqlCommand(string.Join(";", @"SELECT COUNT(*) FROM restriction WHERE guid=@guid",
                            @"SELECT id,from_nodes,to_nodes,via_nodes,vehicle_type FROM restriction WHERE guid=@guid"),
                        connection2))
                {
                    command7.Parameters.AddWithValue("guid", Guid);
                    command7.Prepare();

                    var id = Guid.NewGuid().ToString();
                    await progressClient.Init(id, session);

                    var current = 0;

                    using var reader = await command7.ExecuteReaderAsync();
                    if (!reader.Read()) throw new NullReferenceException();
                    var count = reader.GetInt64(0);
                    reader.NextResult();

                    var doIt = true;
                    var obj = new object();

                    Parallel.For(0, 4, i =>
                    {
                        using (var connection4 = new NpgsqlConnection(ConnectionString))
                        using (var connection5 = new NpgsqlConnection(ConnectionString))
                        {
                            connection4.Open();
                            connection5.Open();

                            using (var command =
                                new NpgsqlCommand(@"BEGIN",
                                    connection4))
                            using (var command3 =
                                new NpgsqlCommand(
                                    @"INSERT INTO restriction_from_edge(rid,edge,vehicle_type,guid) VALUES (@id,@edge,@vehicleType,@guid)",
                                    connection4))
                            using (var command4 =
                                new NpgsqlCommand(
                                    @"INSERT INTO restriction_to_edge(rid,edge,vehicle_type,guid) VALUES (@id,@edge,@vehicleType,@guid)",
                                    connection4))
                            using (var command5 =
                                new NpgsqlCommand(
                                    @"INSERT INTO restriction_via_node(rid,node,vehicle_type,guid) VALUES (@id,@node,@vehicleType,@guid)",
                                    connection4))
                            using (var command6 =
                                new NpgsqlCommand(@"COMMIT",
                                    connection4))
                            using (var command8 =
                                new NpgsqlCommand(
                                    @"SELECT id FROM edge WHERE nodes&&@nodes AND guid=@guid",
                                    connection5))
                            {
                                command3.Parameters.Add("id", NpgsqlDbType.Bigint);
                                command3.Parameters.Add("edge", NpgsqlDbType.Bigint);
                                command3.Parameters.Add("vehicleType", NpgsqlDbType.Varchar);
                                command3.Parameters.AddWithValue("guid", Guid);
                                command3.Prepare();

                                command4.Parameters.Add("id", NpgsqlDbType.Bigint);
                                command4.Parameters.Add("edge", NpgsqlDbType.Bigint);
                                command4.Parameters.Add("vehicleType", NpgsqlDbType.Varchar);
                                command4.Parameters.AddWithValue("guid", Guid);
                                command4.Prepare();

                                command5.Parameters.Add("id", NpgsqlDbType.Bigint);
                                command5.Parameters.Add("node", NpgsqlDbType.Bigint);
                                command5.Parameters.Add("vehicleType", NpgsqlDbType.Varchar);
                                command5.Parameters.AddWithValue("guid", Guid);
                                command5.Prepare();


                                command8.Parameters.Add("nodes", NpgsqlDbType.Array | NpgsqlDbType.Bigint);
                                command8.Parameters.AddWithValue("guid", Guid);
                                command8.Prepare();

                                command.ExecuteNonQuery();

                                while (true)
                                {
                                    long rid;
                                    long[] fromNodes;
                                    long[] toNodes;
                                    long[] viaNodes;
                                    string vehicleType;
                                    lock (obj)
                                    {
                                        if (!doIt) break;
                                        doIt = reader.Read();
                                        if (!doIt) break;
                                        rid = reader.GetInt64(0);
                                        fromNodes = (long[]) reader.GetValue(1);
                                        toNodes = (long[]) reader.GetValue(2);
                                        viaNodes = (long[]) reader.GetValue(3);
                                        vehicleType = reader.GetString(4);
                                    }

                                    command8.Parameters["nodes"].Value = fromNodes;
                                    var fromEdges = new List<long>();
                                    using (var reader5 = command8.ExecuteReader())
                                    {
                                        while (reader5.Read()) fromEdges.Add(reader5.GetInt64(0));
                                    }

                                    command8.Parameters["nodes"].Value = toNodes;
                                    var toEdges = new List<long>();
                                    using (var reader5 = command8.ExecuteReader())
                                    {
                                        while (reader5.Read()) toEdges.Add(reader5.GetInt64(0));
                                    }

                                    fromEdges.ForEach(edge =>
                                    {
                                        command3.Parameters["id"].Value = rid;
                                        command3.Parameters["edge"].Value = edge;
                                        command3.Parameters["vehicleType"].Value = vehicleType;
                                        command3.ExecuteNonQuery();
                                    });

                                    toEdges.ForEach(edge =>
                                    {
                                        command4.Parameters["id"].Value = rid;
                                        command4.Parameters["edge"].Value = edge;
                                        command4.Parameters["vehicleType"].Value = vehicleType;
                                        command4.ExecuteNonQuery();
                                    });

                                    viaNodes.ToList().ForEach(via =>
                                    {
                                        command5.Parameters["id"].Value = rid;
                                        command5.Parameters["node"].Value = via;
                                        command5.Parameters["vehicleType"].Value = vehicleType;
                                        command5.ExecuteNonQuery();
                                    });

                                    lock (obj)
                                    {
                                        if (current++ % 100 == 0)
                                            progressClient.Progress(100f * current / count, id, session).GetAwaiter()
                                                .GetResult();
                                    }
                                }

                                command6.ExecuteNonQuery();
                            }
                        }
                    });

                    await progressClient.Finalize(id, session);
                }

#endregion

                await osmConnection.CloseAsync();
                await osmConnection2.CloseAsync();
                await connection.CloseAsync();
                await connection2.CloseAsync();
                await connection3.CloseAsync();
            }

            async Task ExecuteResourceAsync(Assembly assembly, string resource, NpgsqlConnection connection)
            {
                using var stream = assembly.GetManifestResourceStream(resource);
                using var sr = new StreamReader(stream, Encoding.UTF8);
                using var command = new NpgsqlCommand(await sr.ReadToEndAsync(), connection);
                command.Prepare();
                command.ExecuteNonQuery();
            }
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