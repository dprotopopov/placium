/*
 *  Licensed to SharpSoftware under one or more contributor
 *  license agreements. See the NOTICE file distributed with this work for 
 *  additional information regarding copyright ownership.
 * 
 *  SharpSoftware licenses this file to you under the Apache License, 
 *  Version 2.0 (the "License"); you may not use this file except in 
 *  compliance with the License. You may obtain a copy of the License at
 * 
 *       http://www.apache.org/licenses/LICENSE-2.0
 * 
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using Npgsql;
using NpgsqlTypes;
using OsmSharp;
using OsmSharp.Streams;
using OsmSharp.Streams.Filters;
using Placium.Common;
using Route.Attributes;
using Route.Data.Edges;
using Route.Data.Network;
using Route.Data.Network.Restrictions;
using Route.IO.Osm.Nodes;
using Route.IO.Osm.Normalizer;
using Route.IO.Osm.Relations;
using Route.IO.Osm.Restrictions;
using Route.LocalGeo;
using Route.Logging;
using Route.Profiles;
using EdgeData = Route.Data.Network.Edges.EdgeData;

namespace Route.IO.Osm.Streams
{
    /// <summary>
    ///     A stream target to load a routing database.
    /// </summary>
    public class RouterDbStreamTarget : OsmStreamTarget
    {
        private readonly bool _allCore;
        private readonly RouterDb _db;
        private readonly float _simplifyEpsilonInMeter;
        private readonly Dictionary<long, ushort> _translatedPerOriginal = new Dictionary<long, ushort>();
        private readonly VehicleCache _vehicleCache;
        private readonly HashSet<string> _vehicleTypes;

        private HashSet<ITwoPassProcessor> _anotherPass = new HashSet<ITwoPassProcessor>();

        private bool _firstPass = true; // flag for first/second pass.
        private Dictionary<long, long> _nodeData;
        private NpgsqlCommand _osm_command;

        private NpgsqlConnection _osm_connection;

        // store all the things for when profile translation is enabled, when normalization is turned off.
        private AttributesIndex _profileIndex = new AttributesIndex(AttributesIndexMode.IncreaseOne
                                                                    | AttributesIndexMode.ReverseAll);

        private NpgsqlCommand _route_command;
        private NpgsqlCommand _route_command2;
        private NpgsqlConnection _route_connection;
        private NpgsqlConnection _route_connection2;

        private TextWriter _writer;
        private TextWriter _writer2;

        /// <summary>
        ///     Creates a new router db stream target.
        /// </summary>
        public RouterDbStreamTarget(RouterDb db, Vehicle[] vehicles, bool allCore = false,
            int minimumStages = 1, IEnumerable<ITwoPassProcessor> processors = null, bool processRestrictions = false,
            float simplifyEpsilonInMeter = .1f)
            : this(db, new VehicleCache(vehicles), allCore, minimumStages, processors, processRestrictions,
                simplifyEpsilonInMeter)
        {
        }

        /// <summary>
        ///     Creates a new router db stream target.
        /// </summary>
        public RouterDbStreamTarget(RouterDb db, VehicleCache vehicleCache, bool allCore = false,
            int minimumStages = 1, IEnumerable<ITwoPassProcessor> processors = null, bool processRestrictions = false,
            float simplifyEpsilonInMeter = .1f)
        {
            _db = db;
            _allCore = allCore;
            _simplifyEpsilonInMeter = simplifyEpsilonInMeter;

            _vehicleCache = vehicleCache;
            _vehicleTypes = new HashSet<string>();

            foreach (var vehicle in _vehicleCache.Vehicles)
            foreach (var vehicleType in vehicle.VehicleTypes)
                _vehicleTypes.Add(vehicleType);

            foreach (var vehicle in _vehicleCache.Vehicles) db.AddSupportedVehicle(vehicle);

            if (processors == null) processors = new List<ITwoPassProcessor>();
            Processors = new List<ITwoPassProcessor>(processors);

            InitializeDefaultProcessors(processRestrictions);
        }

        /// <summary>
        ///     Gets or sets extra two-pass processors.
        /// </summary>
        public List<ITwoPassProcessor> Processors { get; set; }

        /// <summary>
        ///     Gets or sets a flag to keep node id's.
        /// </summary>
        /// <remarks>This is a way to build 'stable' identifiers for each vertex.</remarks>
        public bool KeepNodeIds { get; set; }

        /// <summary>
        ///     Gets the normalize flag.
        /// </summary>
        public bool Normalize { get; private set; }

        /// <summary>
        ///     Gets or sets a flag to keep way id's and the index of the first node of the edge in the original way.
        /// </summary>
        /// <returns></returns>
        /// <remarks>This is a way to build 'stable' identifiers for each segment.</remarks>
        public bool KeepWayIds { get; set; }

        /// <summary>
        ///     Setups default add-on processors.
        /// </summary>
        private void InitializeDefaultProcessors(bool processRestrictions)
        {
            // add all vehicle relation processors.
            var isDynamic = false;
            foreach (var vehicle in _vehicleCache.Vehicles)
            {
                if (!(vehicle is DynamicVehicle dynamicVehicle)) continue;

                isDynamic = true;
                Processors.Add(new DynamicVehicleRelationTagProcessor(dynamicVehicle));
                Processors.Add(new DynamicVehicleNodeTagProcessor(_db, dynamicVehicle, MarkCore));
                if (processRestrictions)
                    Processors.Add(
                        new DynamicVehicleNodeRestrictionProcessor(_db, dynamicVehicle, MarkCore, FoundRestriction));
            }

            // add restriction processor if needed.
            if (processRestrictions)
            {
                // add node-based restriction processor if non of the profiles is dynamic.
                if (!isDynamic) Processors.Add(new NodeRestrictionProcessor(MarkCore, FoundRestriction));

                Processors.Add(new RestrictionProcessor(_vehicleTypes,
                    node => _nodeData.ContainsKey(node) ? _nodeData[node] : long.MaxValue, MarkCore,
                    FoundRestriction));
            }

            // a function to handle callbacks from processors handling restrictions.
            void FoundRestriction(string vehicleType, List<long> sequence)
            {
                if (vehicleType == null) vehicleType = string.Empty;

                var restrictionValues = new[]
                {
                    _db.Guid.ToString(),
                    vehicleType,
                    $"{{{string.Join(",", sequence.Select(t => $"{t}"))}}}"
                };

                _writer2.WriteLine(string.Join("\t", restrictionValues));
            }

            // a function to handle callbacks from processor that want to mark nodes as core.
            long MarkCore(Node node)
            {
                if (_firstPass)
                {
                    var nodeValues = new[]
                    {
                        _db.Guid.ToString(),
                        node.Id.ToString(),
                        node.Latitude.ValueAsText(),
                        node.Longitude.ValueAsText(),
                        true.ValueAsText()
                    };

                    _writer.WriteLine(string.Join("\t", nodeValues));
                    return global::Route.Constants.NO_VERTEX;
                }

                return _nodeData[node.Id.Value];
            }
        }

        /// <summary>
        ///     Initializes this target.
        /// </summary>
        public override void Initialize()
        {
            _firstPass = true;
        }

        /// <summary>
        ///     Called right before pull and right after initialization.
        /// </summary>
        /// <returns></returns>
        public override bool OnBeforePull()
        {
            if (_route_connection == null)
            {
                _route_connection = new NpgsqlConnection(_db.RouteConnectionString);
                _route_connection.Open();
            }

            if (_writer == null)
            {
                ExecuteResource(Assembly.GetExecutingAssembly(),
                    "Route.IO.Osm.Streams.CreateTempTables.pgsql",
                    _route_connection);
                _writer = _route_connection.BeginTextImport(
                    "COPY temp_node (guid,id,latitude,longitude,is_core) FROM STDIN WITH NULL AS ''");
            }

            if (_route_connection2 == null)
            {
                _route_connection2 = new NpgsqlConnection(_db.RouteConnectionString);
                _route_connection2.Open();
            }

            if (_writer2 == null)
            {
                ExecuteResource(Assembly.GetExecutingAssembly(),
                    "Route.IO.Osm.Streams.CreateTempTables2.pgsql",
                    _route_connection2);
                _writer2 = _route_connection2.BeginTextImport(
                    "COPY temp_restriction (guid,vehicle_type,nodes) FROM STDIN WITH NULL AS ''");
            }

            // execute the first pass.
            DoPull();

            // do extra relation passes if needed.
            while (_anotherPass.Count > 0)
            {
                var anotherPass = new HashSet<ITwoPassProcessor>();
                Source.Reset();
                while (Source.MoveNext(true, true, false))
                    foreach (var processor in _anotherPass)
                        if (processor.FirstPass(Source.Current() as Relation))
                            anotherPass.Add(processor);
                _anotherPass = anotherPass;
            }

            if (_writer != null)
            {
                _writer.Flush();
                _writer.Dispose();
                _writer = null;
                ExecuteResource(Assembly.GetExecutingAssembly(),
                    "Route.IO.Osm.Streams.InsertFromTempTables.pgsql",
                    _route_connection);
                using var command =
                    new NpgsqlCommand(
                        @"SELECT id,latitude,longitude,ROW_NUMBER() OVER (ORDER BY id) FROM node WHERE guid=@guid AND is_core",
                        _route_connection);
                command.Parameters.Add("guid", NpgsqlDbType.Uuid);
                command.Prepare();
                command.Parameters["guid"].Value = _db.Guid;
                var keyValuePairs = new List<KeyValuePair<long, long>>();
                using var reader = command.ExecuteReader();
                while (reader.Read())
                {
                    var id = reader.GetInt64(0);
                    var latitude = (float) reader.GetDouble(1);
                    var longitude = (float) reader.GetDouble(2);
                    var vertex = reader.GetInt64(3) - 1;
                    _db.Network.AddVertex(vertex, latitude, longitude);
                    keyValuePairs.Add(new KeyValuePair<long, long>(id, vertex));
                }

                _nodeData = keyValuePairs.ToDictionary(x => x.Key, x => x.Value);
            }

            // move to second pass.
            _firstPass = false;

            Source.Reset();
            DoPull();

            if (_writer2 != null)
            {
                _writer2.Flush();
                _writer2.Dispose();
                _writer2 = null;
                ExecuteResource(Assembly.GetExecutingAssembly(),
                    "Route.IO.Osm.Streams.InsertFromTempTables2.pgsql",
                    _route_connection2);
                using var command =
                    new NpgsqlCommand(
                        @"SELECT vehicle_type,nodes FROM restriction WHERE guid=@guid",
                        _route_connection2);
                command.Parameters.Add("guid", NpgsqlDbType.Uuid);
                command.Prepare();
                command.Parameters["guid"].Value = _db.Guid;
                using var reader = command.ExecuteReader();
                while (reader.Read())
                {
                    var vehicleType = reader.GetString(0);
                    var nodes = (long[]) reader.GetValue(1);
                    if (!_db.TryGetRestrictions(vehicleType, out var restrictions))
                    {
                        restrictions = new RestrictionsDb();
                        _db.AddRestrictions(vehicleType, restrictions);
                    }
                    restrictions.Add(nodes.Select(x => _nodeData[x]).ToArray());
                }
            }

            Logger.Log("RouterDbStreamTarget", TraceEventType.Information,
                $"{_db.EdgeProfiles.Count} profiles.");

            return false;
        }

        /// <summary>
        ///     Registers the source.
        /// </summary>
        public virtual void RegisterSource(OsmStreamSource source, bool normalize)
        {
            Normalize = normalize;

            if (normalize)
            {
                // add normalization and filtering.
                _profileIndex = _db.EdgeProfiles;

                var eventsFilter = new OsmStreamFilterDelegate();
                eventsFilter.MoveToNextEvent += (osmGeo, param) =>
                {
                    if (osmGeo.Type == OsmGeoType.Way)
                    {
                        // normalize tags, reduce the combination of tags meaning the same thing.
                        var tags = osmGeo.Tags.ToAttributes();
                        var normalizedTags = new AttributeCollection();
                        if (!DefaultTagNormalizer.Normalize(tags, normalizedTags, _vehicleCache))
                            // invalid data, no access, or tags make no sense at all.
                            return osmGeo;

                        // rewrite tags and keep whitelisted meta-tags.
                        osmGeo.Tags.Clear();
                        foreach (var tag in normalizedTags) osmGeo.Tags.Add(tag.Key, tag.Value);
                        foreach (var tag in tags)
                            if (_vehicleCache.Vehicles.IsOnMetaWhiteList(tag.Key))
                                osmGeo.Tags.Add(tag.Key, tag.Value);
                    }

                    return osmGeo;
                };
                eventsFilter.RegisterSource(source);

                base.RegisterSource(eventsFilter);
            }
            else
            {
                // no filtering.
                base.RegisterSource(source);
            }
        }

        /// <summary>
        ///     Registers the source.
        /// </summary>
        public override void RegisterSource(OsmStreamSource source)
        {
            RegisterSource(source, true);
        }

        /// <summary>
        ///     Adds a node.
        /// </summary>
        public override void AddNode(Node node)
        {
            if (_firstPass)
            {
                if (Processors != null)
                    foreach (var processor in Processors)
                        processor.FirstPass(node);
            }
            else
            {
                if (Processors != null)
                    foreach (var processor in Processors)
                        processor.SecondPass(node);
            }
        }

        /// <summary>
        ///     Adds a way.
        /// </summary>
        public override void AddWay(Way way)
        {
            if (way == null) return;
            if (way.Nodes == null) return;
            if (way.Nodes.Length == 0) return;
            if (way.Tags == null || way.Tags.Count == 0) return;

            if (_firstPass)
            {
                // just keep.
                if (Processors != null)
                    foreach (var processor in Processors)
                        processor.FirstPass(way);

                if (_vehicleCache.AnyCanTraverse(way.Tags.ToAttributes()))
                    if (true)
                    {
                        if (!_vehicleCache.AnyCanTraverse(way.Tags.ToAttributes())) return;

                        if (_osm_connection == null)
                        {
                            _osm_connection = new NpgsqlConnection(_db.OsmConnectionString);
                            _osm_connection.Open();
                        }

                        if (_osm_command == null)
                        {
                            _osm_command = new NpgsqlCommand(
                                @"SELECT id,latitude,longitude FROM node WHERE id=ANY(@ids)",
                                _osm_connection);
                            _osm_command.Parameters.Add("ids", NpgsqlDbType.Array | NpgsqlDbType.Bigint);
                            _osm_command.Prepare();
                        }

                        _osm_command.Parameters["ids"].Value = way.Nodes;

                        var list = new List<NodeItem>(way.Nodes.Length);

                        using (var reader = _osm_command.ExecuteReader())
                        {
                            while (reader.Read())
                                list.Add(new NodeItem
                                {
                                    Id = reader.GetInt64(0),
                                    Latitude = reader.GetDouble(1),
                                    Longitude = reader.GetDouble(2)
                                });
                        }

                        list.ForEach(node =>
                        {
                            var nodeValues = new[]
                            {
                                _db.Guid.ToString(),
                                node.Id.ToString(),
                                node.Latitude.ValueAsText(),
                                node.Longitude.ValueAsText(),
                                (node.Id == way.Nodes.First() || node.Id == way.Nodes.Last()).ValueAsText()
                            };

                            _writer.WriteLine(string.Join("\t", nodeValues));
                        });
                    }
            }
            else
            {
                if (Processors != null)
                    foreach (var processor in Processors)
                        processor.SecondPass(way);

                var wayAttributes = way.Tags.ToAttributes();
                var profileWhiteList = new Whitelist();
                if (_vehicleCache.AddToWhiteList(wayAttributes, profileWhiteList))
                {
                    // way has some use.
                    // build profile and meta-data.
                    var profileTags = new AttributeCollection();
                    var metaTags = new AttributeCollection();
                    foreach (var tag in way.Tags)
                        if (profileWhiteList.Contains(tag.Key))
                            profileTags.Add(tag);
                        else if (_vehicleCache.Vehicles.IsOnProfileWhiteList(tag.Key))
                            profileTags.Add(tag);
                        else if (_vehicleCache.Vehicles.IsOnMetaWhiteList(tag.Key)) metaTags.Add(tag);

                    if (!_vehicleCache.AnyCanTraverse(profileTags))
                        // way has some use, add all of it's nodes to the index.
                        return;

                    // get profile and meta-data id's.
                    var profileCount = _profileIndex.Count;
                    var profile = _profileIndex.Add(profileTags);
                    if (profileCount != _profileIndex.Count)
                    {
                        var stringBuilder = new StringBuilder();

                        if (!Normalize)
                        {
                            // no normalization, translate profiles by.
                            metaTags.AddOrReplace(profileTags);

                            // translate profile.
                            var translatedProfile = new AttributeCollection();
                            translatedProfile.AddOrReplace("translated_profile", "yes");
                            foreach (var vehicle in _vehicleCache.Vehicles)
                            foreach (var vehicleProfile in vehicle.GetProfiles())
                            {
                                var factorAndSpeed = vehicleProfile.FactorAndSpeed(profileTags);
                                translatedProfile.AddOrReplace($"{vehicleProfile.FullName}",
                                    $"{factorAndSpeed.Direction}|" +
                                    global::Route.Extensions.ToInvariantString(factorAndSpeed.Value) + "|" +
                                    global::Route.Extensions.ToInvariantString(factorAndSpeed.SpeedFactor));
                            }

                            var translatedCount = _db.EdgeProfiles.Count;
                            var translatedProfileId = _db.EdgeProfiles.Add(translatedProfile);
                            if (translatedProfileId > EdgeDataSerializer.MAX_PROFILE_COUNT)
                                throw new Exception(
                                    "Maximum supported profiles exceeded, make sure only routing tags are included in the profiles.");
                            _translatedPerOriginal[profile] = (ushort) translatedProfileId;
                            profile = translatedProfileId;
                            if (translatedCount != _db.EdgeProfiles.Count)
                            {
                                stringBuilder.Clear();
                                foreach (var att in translatedProfile)
                                {
                                    stringBuilder.Append(att.Key);
                                    stringBuilder.Append('=');
                                    stringBuilder.Append(att.Value);
                                    stringBuilder.Append(' ');
                                }

                                Logger.Log("RouterDbStreamTarget", TraceEventType.Information,
                                    "New translated profile: # {0}: {1}", _db.EdgeProfiles.Count,
                                    global::Route.Extensions.ToInvariantString(stringBuilder));
                            }
                        }
                        else
                        {
                            foreach (var att in profileTags)
                            {
                                stringBuilder.Append(att.Key);
                                stringBuilder.Append('=');
                                stringBuilder.Append(att.Value);
                                stringBuilder.Append(' ');
                            }

                            Logger.Log("RouterDbStreamTarget", TraceEventType.Information,
                                "New edge profile: # profiles {0}: {1}", _profileIndex.Count,
                                global::Route.Extensions.ToInvariantString(stringBuilder));

                            if (profile > EdgeDataSerializer.MAX_PROFILE_COUNT)
                                throw new Exception(
                                    "Maximum supported profiles exceeded, make sure only routing tags are included in the profiles.");
                        }
                    }
                    else if (!Normalize)
                    {
                        profile = _translatedPerOriginal[profile];

                        metaTags.AddOrReplace(profileTags);
                    }

                    var meta = _db.EdgeMeta.Add(metaTags);

                    if (!_vehicleCache.AnyCanTraverse(way.Tags.ToAttributes())) return;

                    if (_route_connection == null)
                    {
                        _route_connection = new NpgsqlConnection(_db.RouteConnectionString);
                        _route_connection.Open();
                    }

                    if (_route_command == null)
                    {
                        _route_command = new NpgsqlCommand(
                            @"SELECT id,latitude,longitude,is_core FROM node WHERE id=ANY(@ids) AND guid=@guid",
                            _route_connection);
                        _route_command.Parameters.Add("ids", NpgsqlDbType.Array | NpgsqlDbType.Bigint);
                        _route_command.Parameters.Add("guid", NpgsqlDbType.Uuid);
                        _route_command.Prepare();
                    }

                    _route_command.Parameters["ids"].Value = way.Nodes;
                    _route_command.Parameters["guid"].Value = _db.Guid;


                    var list = new List<NodeItem>(way.Nodes.Length);

                    using (var reader = _route_command.ExecuteReader())
                    {
                        while (reader.Read())
                            list.Add(new NodeItem
                            {
                                Id = reader.GetInt64(0),
                                Latitude = (float) reader.GetDouble(1),
                                Longitude = (float) reader.GetDouble(2),
                                IsCore = reader.GetBoolean(3)
                            });
                    }


                    var dictionary = list.ToDictionary(item => item.Id, item => item);

                    // convert way into one or more edges.
                    var node = 0;
                    var fromNodeIdx = 0;

                    while (node < way.Nodes.Length - 1)
                    {
                        fromNodeIdx = node;

                        // build edge to add.
                        var intermediates = new List<Coordinate>();
                        var distance = 0.0f;
                        if (!dictionary.ContainsKey(way.Nodes[node])) return;
                        var item = dictionary[way.Nodes[node]];

                        var coordinate = new Coordinate((float) item.Latitude, (float) item.Longitude);

                        var fromVertex = _nodeData[way.Nodes[node]];
                        var fromNode = way.Nodes[node];
                        var previousCoordinate = coordinate;
                        node++;

                        var toVertex = long.MaxValue;
                        var toNode = long.MaxValue;
                        while (true)
                        {
                            if (node >= way.Nodes.Length ||
                                !dictionary.ContainsKey(way.Nodes[node]))
                                // an incomplete way, node not in source.
                                return;

                            item = dictionary[way.Nodes[node]];

                            coordinate = new Coordinate((float) item.Latitude, (float) item.Longitude);

                            distance += Coordinate.DistanceEstimateInMeter(
                                previousCoordinate, coordinate);

                            if (item.IsCore)
                            {
                                // node is part of the core.
                                toVertex = _nodeData[way.Nodes[node]];
                                toNode = way.Nodes[node];
                                break;
                            }

                            intermediates.Add(coordinate);
                            previousCoordinate = coordinate;
                            node++;
                        }

                        // just add edge.
                        // duplicates are allowed, one-edge loops are allowed, and too long edges are added with max-length.
                        // these data-issues are fixed in another processing step.
                        AddCoreEdge(fromVertex, toVertex, new EdgeData
                        {
                            MetaId = meta,
                            Distance = distance,
                            Profile = (ushort) profile
                        }, intermediates, way.Id.Value, (ushort) fromNodeIdx);
                    }
                }
            }
        }

        protected void ExecuteResource(Assembly assembly, string resource, NpgsqlConnection connection)
        {
            using var stream = assembly.GetManifestResourceStream(resource);
            using var sr = new StreamReader(stream, Encoding.UTF8);
            using var command = new NpgsqlCommand(sr.ReadToEnd(), connection);

            command.Prepare();

            command.ExecuteNonQuery();
        }

        /// <summary>
        ///     Adds a new edge.
        /// </summary>
        public void AddCoreEdge(long vertex1, long vertex2, EdgeData data, List<Coordinate> shape, long wayId,
            ushort nodeIdx)
        {
            if (data.Distance >= _db.Network.MaxEdgeDistance)
                // distance is too big to fit into the graph's data field.
                // just add the edge with the max distance, length can be recalculated on the fly for these edges 
                // or (and this is what's probably done) we split up the edge later and add a proper length.
                data = new EdgeData
                {
                    Distance = _db.Network.MaxEdgeDistance,
                    Profile = data.Profile,
                    MetaId = data.MetaId
                };

            // add the edge.
            var edgeId = _db.Network.AddEdge(vertex1, vertex2, data,
                shape.Simplify(_simplifyEpsilonInMeter));
        }

        /// <summary>
        ///     Adds a relation.
        /// </summary>
        public override void AddRelation(Relation relation)
        {
            if (_firstPass)
            {
                if (Processors != null)
                    foreach (var processor in Processors)
                        if (processor.FirstPass(relation))
                            _anotherPass.Add(processor);
            }
            else
            {
                if (Processors != null)
                    foreach (var processor in Processors)
                        processor.SecondPass(relation);
            }
        }

        public class NodeItem
        {
            public long Id { get; set; }
            public double Latitude { get; set; }
            public double Longitude { get; set; }
            public bool IsCore { get; set; }
        }
    }
}