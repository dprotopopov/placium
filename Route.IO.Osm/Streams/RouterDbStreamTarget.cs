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
using Route.IO.Osm.Nodes;
using Route.IO.Osm.Normalizer;
using Route.IO.Osm.Relations;
using Route.IO.Osm.Restrictions;
using Route.LocalGeo;
using Route.Logging;
using Route.Profiles;

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
        private readonly VehicleCache _vehicleCache;
        private readonly HashSet<string> _vehicleTypes;

        private HashSet<ITwoPassProcessor> _anotherPass = new HashSet<ITwoPassProcessor>();

        private bool _firstPass = true; // flag for first/second pass.
        private NpgsqlCommand _osm_command;

        private NpgsqlConnection _osm_connection;

        // store all the things for when profile translation is enabled, when normalization is turned off.
        private AttributesIndex _profileIndex = new AttributesIndex(AttributesIndexMode.IncreaseOne
                                                                    | AttributesIndexMode.ReverseAll);

        private NpgsqlCommand _route_command;
        private NpgsqlCommand _route_command2;
        private NpgsqlCommand _route_command3;
        private NpgsqlConnection _route_connection;
        private NpgsqlConnection _route_connection2;
        private NpgsqlConnection _route_connection3;
        private NpgsqlConnection _route_connection4;

        private TextWriter _writer;
        private TextWriter _writer2;
        private TextWriter _writer3;
        private TextWriter _writer4;

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
                Processors.Add(new DynamicVehicleNodeTagProcessor(_db, dynamicVehicle, MarkCore, AddMeta));
                if (processRestrictions)
                    Processors.Add(
                        new DynamicVehicleNodeRestrictionProcessor(_db, dynamicVehicle, MarkCore, FoundRestriction,
                            AddMeta));
            }

            // add restriction processor if needed.
            if (processRestrictions)
            {
                // add node-based restriction processor if non of the profiles is dynamic.
                if (!isDynamic) Processors.Add(new NodeRestrictionProcessor(MarkCore, FoundRestriction));

                Processors.Add(new RestrictionProcessor(_vehicleTypes,
                    node => _db.NodeData.TryGetValue(node, out var vertex) ? vertex : long.MaxValue, MarkCore,
                    FoundRestriction));
            }

            // a function to handle callbacks from processors handling restrictions.
            void FoundRestriction(string vehicleType, List<long> sequence)
            {
                if (vehicleType == null) vehicleType = string.Empty;

                var values = new[]
                {
                    _db.Guid.ToString(),
                    vehicleType,
                    $"{{{string.Join(",", sequence.Select(t => $"{t}"))}}}"
                };

                _writer2.WriteLine(string.Join("\t", values));
            }

            // a function to handle callbacks from processor that want to mark nodes as core.
            long MarkCore(Node node)
            {
                if (_firstPass)
                {
                    var values = new[]
                    {
                        _db.Guid.ToString(),
                        node.Id.ToString(),
                        node.Latitude.ValueAsText(),
                        node.Longitude.ValueAsText(),
                        true.ValueAsText()
                    };

                    _writer.WriteLine(string.Join("\t", values));
                    return global::Route.Constants.NO_VERTEX;
                }

                return _db.NodeData[node.Id.Value];
            }

            long AddMeta(Node node, IAttributeCollection attributes)
            {
                if (_firstPass)
                {
                    var values = new[]
                    {
                        _db.Guid.ToString(),
                        node.Id.ToString(),
                        $"{string.Join(",", attributes.Select(t => $"\"{t.Key.TextEscape(2)}\"=>\"{t.Value.TextEscape(2)}\""))}"
                    };

                    _writer4.WriteLine(string.Join("\t", values));
                    return global::Route.Constants.NO_VERTEX;
                }

                var vertex = _db.NodeData[node.Id.Value];
                _db.VertexMeta[vertex] = attributes;
                return vertex;
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


            if (_route_connection3 == null)
            {
                _route_connection3 = new NpgsqlConnection(_db.RouteConnectionString);
                _route_connection3.Open();
                _route_connection3.TypeMapper.MapComposite<RouteCoordinate>("coordinate");
            }

            if (_writer3 == null)
            {
                ExecuteResource(Assembly.GetExecutingAssembly(),
                    "Route.IO.Osm.Streams.CreateTempTables3.pgsql",
                    _route_connection3);
                _writer3 = _route_connection3.BeginTextImport(
                    "COPY temp_edge (guid,from_node,to_node,distance,coordinates,meta_tags,profile_tags) FROM STDIN WITH NULL AS ''");
            }

            if (_route_connection4 == null)
            {
                _route_connection4 = new NpgsqlConnection(_db.RouteConnectionString);
                _route_connection4.Open();
            }

            if (_writer4 == null)
            {
                ExecuteResource(Assembly.GetExecutingAssembly(),
                    "Route.IO.Osm.Streams.CreateTempTables4.pgsql",
                    _route_connection4);
                _writer4 = _route_connection4.BeginTextImport(
                    "COPY temp_meta (guid,node,tags) FROM STDIN WITH NULL AS ''");
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

                _db.LoadVertexes();
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
                _db.LoadRestrictions();
            }


            if (_writer3 != null)
            {
                _writer3.Flush();
                _writer3.Dispose();
                _writer3 = null;
                ExecuteResource(Assembly.GetExecutingAssembly(),
                    "Route.IO.Osm.Streams.InsertFromTempTables3.pgsql",
                    _route_connection3);
                _db.LoadEdges(Normalize);
            }

            if (_writer4 != null)
            {
                _writer4.Flush();
                _writer4.Dispose();
                _writer4 = null;
                ExecuteResource(Assembly.GetExecutingAssembly(),
                    "Route.IO.Osm.Streams.InsertFromTempTables4.pgsql",
                    _route_connection4);
                _db.LoadVertexMeta();
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
                            Latitude = reader.GetFloat(1),
                            Longitude = reader.GetFloat(2)
                        });
                }

                list.ForEach(node =>
                {
                    var values = new[]
                    {
                        _db.Guid.ToString(),
                        node.Id.ToString(),
                        node.Latitude.ValueAsText(),
                        node.Longitude.ValueAsText(),
                        (node.Id == way.Nodes.First() || node.Id == way.Nodes.Last()).ValueAsText()
                    };

                    _writer.WriteLine(string.Join("\t", values));
                });
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
                                Latitude = reader.GetFloat(1),
                                Longitude = reader.GetFloat(2),
                                IsCore = reader.GetBoolean(3)
                            });
                    }


                    var dictionary = list.ToDictionary(item => item.Id, item => item);

                    // convert way into one or more edges.
                    var node = 0;

                    while (node < way.Nodes.Length - 1)
                    {
                        // build edge to add.
                        var intermediates = new List<Coordinate>();
                        var distance = 0.0f;
                        if (!dictionary.TryGetValue(way.Nodes[node], out var item)) return;

                        var coordinate = new Coordinate(item.Latitude, item.Longitude);

                        var fromVertex = _db.NodeData[way.Nodes[node]];
                        var fromNode = way.Nodes[node];
                        var previousCoordinate = coordinate;
                        node++;

                        var toVertex = long.MaxValue;
                        var toNode = long.MaxValue;
                        while (true)
                        {
                            if (node >= way.Nodes.Length ||
                                !dictionary.TryGetValue(way.Nodes[node], out item))
                                // an incomplete way, node not in source.
                                return;

                            coordinate = new Coordinate(item.Latitude, item.Longitude);

                            distance += Coordinate.DistanceEstimateInMeter(
                                previousCoordinate, coordinate);

                            if (item.IsCore)
                            {
                                // node is part of the core.
                                toVertex = _db.NodeData[way.Nodes[node]];
                                toNode = way.Nodes[node];
                                break;
                            }

                            intermediates.Add(coordinate);
                            previousCoordinate = coordinate;
                            node++;
                        }

                        var values = new[]
                        {
                            _db.Guid.ToString(),
                            fromNode.ToString(),
                            toNode.ToString(),
                            distance.ValueAsText(),
                            $"{{{string.Join(",", intermediates.Select(t => $"\\\"({t.Latitude.ValueAsText()},{t.Longitude.ValueAsText()})\\\""))}}}",
                            $"{string.Join(",", metaTags.Select(t => $"\"{t.Key.TextEscape(2)}\"=>\"{t.Value.TextEscape(2)}\""))}",
                            $"{string.Join(",", profileTags.Select(t => $"\"{t.Key.TextEscape(2)}\"=>\"{t.Value.TextEscape(2)}\""))}"
                        };

                        _writer3.WriteLine(string.Join("\t", values));
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
            public float Latitude { get; set; }
            public float Longitude { get; set; }
            public bool IsCore { get; set; }
        }
    }
}