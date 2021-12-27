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
using Placium.Common;
using Placium.Route.Profiles;
using Route.Attributes;
using Route.LocalGeo;

namespace Placium.Route
{
    public class RouterDb
    {
        public RouterDb(Guid guid, string connectionString,
            Vehicle[] vehicles)
        {
            Guid = guid;
            ConnectionString = connectionString;
            VehicleCache = new VehicleCache(vehicles);
            EdgeProfiles = new AttributesIndex(AttributesIndexMode.IncreaseOne
                                               | AttributesIndexMode.ReverseAll);
        }

        public AttributesIndex EdgeProfiles { get; set; }

        public VehicleCache VehicleCache { get; }
        public Guid Guid { get; }
        public string ConnectionString { get; }

        public async Task LoadFromOsmAsync(string osmConnectionString, IProgressClient progressClient, string session)
        {
            using (var osmConnection = new NpgsqlConnection(osmConnectionString))
            using (var osmConnection2 = new NpgsqlConnection(osmConnectionString))
            using (var connection = new NpgsqlConnection(ConnectionString))
            using (var connection2 = new NpgsqlConnection(ConnectionString))
            {
                await osmConnection.OpenAsync();
                await osmConnection2.OpenAsync();
                await connection.OpenAsync();
                await connection2.OpenAsync();

                var id = Guid.NewGuid().ToString();
                await progressClient.Init(id, session);

                await ExecuteResourceAsync(Assembly.GetExecutingAssembly(),
                    "Placium.Route.CreateTempTables.pgsql",
                    connection);
                using (var writer = connection.BeginTextImport(
                    "COPY temp_node (guid,id,latitude,longitude,is_core) FROM STDIN WITH NULL AS ''"))
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
                using (var command2 = new NpgsqlCommand(@"SELECT id,latitude,longitude FROM node WHERE id=ANY(@ids)",
                    osmConnection2))
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
                                var nodeId = reader2.GetInt64(0);
                                var latitude = reader2.GetFloat(1);
                                var longitude = reader2.GetFloat(2);

                                var values = new[]
                                {
                                    Guid.ToString(),
                                    nodeId.ToString(),
                                    latitude.ValueAsText(),
                                    longitude.ValueAsText(),
                                    (nodeId == way.Nodes.First() || nodeId == way.Nodes.Last()).ValueAsText()
                                };

                                writer.WriteLine(string.Join("\t", values));
                            }
                        }

                        if (current++ % 1000 == 0)
                            await progressClient.Progress(100f * current / count, id, session);
                    }
                }

                await ExecuteResourceAsync(Assembly.GetExecutingAssembly(),
                    "Placium.Route.InsertFromTempTables.pgsql",
                    connection);
                await progressClient.Finalize(id, session);

                id = Guid.NewGuid().ToString();
                await progressClient.Init(id, session);

                await ExecuteResourceAsync(Assembly.GetExecutingAssembly(),
                    "Placium.Route.CreateTempTables3.pgsql",
                    connection);

                using (var writer = connection.BeginTextImport(
                    "COPY temp_edge (guid,from_node,to_node,distance,coordinates,location,tags,direction,weight) FROM STDIN WITH NULL AS ''")
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
                using (var command2 = new NpgsqlCommand(
                    @"SELECT id,latitude,longitude,is_core FROM node WHERE id=ANY(@ids) AND guid=@guid",
                    connection2))
                {
                    command2.Parameters.Add("ids", NpgsqlDbType.Array | NpgsqlDbType.Bigint);
                    command2.Parameters.AddWithValue("guid", Guid);
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
                            var factorAndSpeeds = new Dictionary<string, FactorAndSpeed>();
                            foreach (var vehicle in VehicleCache.Vehicles)
                            foreach (var profile in vehicle.GetProfiles())
                                factorAndSpeeds.Add(profile.FullName, profile.FactorAndSpeed(attributes));

                            command2.Parameters["ids"].Value = way.Nodes;

                            var list = new List<NodeItem>(way.Nodes.Length);

                            using (var reader2 = await command2.ExecuteReaderAsync())
                            {
                                while (reader2.Read())
                                    list.Add(new NodeItem
                                    {
                                        Id = reader2.GetInt64(0),
                                        Latitude = reader2.GetFloat(1),
                                        Longitude = reader2.GetFloat(2),
                                        IsCore = reader2.GetBoolean(3)
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
                                if (!dictionary.TryGetValue(way.Nodes[i], out var item)) return;


                                var previousCoordinate = new Coordinate(item.Latitude, item.Longitude);
                                intermediates.Add(previousCoordinate);

                                var fromNode = way.Nodes[i];
                                i++;

                                var toNode = long.MaxValue;
                                while (true)
                                {
                                    if (i >= way.Nodes.Length ||
                                        !dictionary.TryGetValue(way.Nodes[i], out item))
                                        // an incomplete way, node not in source.
                                        return;

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

                                var direction = factorAndSpeeds.ToDictionary(x => x.Key, x => x.Value.Direction);
                                var weight = factorAndSpeeds.Where(x => x.Value.Value > 0)
                                    .ToDictionary(x => x.Key, x => distance / x.Value.Value);

                                var values = new[]
                                {
                                    Guid.ToString(),
                                    fromNode.ToString(),
                                    toNode.ToString(),
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

                await osmConnection.CloseAsync();
                await osmConnection2.CloseAsync();
                await connection.CloseAsync();
                await connection2.CloseAsync();
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