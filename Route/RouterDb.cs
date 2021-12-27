using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Npgsql;
using NpgsqlTypes;
using Reminiscence.IO.Streams;
using Route.Attributes;
using Route.Data.Contracted;
using Route.Data.Edges;
using Route.Data.Meta;
using Route.Data.Network;
using Route.Data.Network.Restrictions;
using Route.Data.Shortcuts;
using Route.Graphs.Geometric;
using Route.LocalGeo;
using Route.Logging;
using Route.Profiles;
using EdgeData = Route.Data.Network.Edges.EdgeData;

namespace Route
{
    /// <summary>
    ///     Represents a routing database.
    /// </summary>
    public class RouterDb
    {
        private readonly Dictionary<string, ContractedDb> _contracted;
        private readonly Dictionary<string, RestrictionsDb> _restrictionDbs;
        private readonly Dictionary<string, ShortcutsDb> _shortcutsDbs;
        private readonly Dictionary<string, Profile> _supportedProfiles;
        private readonly Dictionary<string, Vehicle> _supportedVehicles;
        private readonly Dictionary<long, ushort> _translatedPerOriginal = new Dictionary<long, ushort>();

        /// <summary>
        ///     Creates a new router database.
        /// </summary>
        public RouterDb(string routeConnectionString, string osmConnectionString,
            float maxEdgeDistance = Constants.DefaultMaxEdgeDistance)
        {
            RouteConnectionString = routeConnectionString;
            OsmConnectionString = osmConnectionString;
            Network = new RoutingNetwork(new GeometricGraph(1), maxEdgeDistance);
            EdgeProfiles = new AttributesIndex(AttributesIndexMode.IncreaseOne
                                               | AttributesIndexMode.ReverseAll);
            EdgeMeta = new AttributesIndex(AttributesIndexMode.ReverseStringIndexKeysOnly);
            VertexMeta = new MappedAttributesIndex();
            VertexData = new MetaCollectionDb();
            EdgeData = new MetaCollectionDb();
            Meta = new AttributeCollection();

            _supportedVehicles = new Dictionary<string, Vehicle>();
            _supportedProfiles = new Dictionary<string, Profile>();
            _contracted = new Dictionary<string, ContractedDb>();
            _restrictionDbs = new Dictionary<string, RestrictionsDb>();
            _shortcutsDbs = new Dictionary<string, ShortcutsDb>();

            Guid = Guid.NewGuid();
        }
        /// <summary>
        ///     Creates a new router database.
        /// </summary>
        public RouterDb(Guid guid, string routeConnectionString, string osmConnectionString,
            float maxEdgeDistance = Constants.DefaultMaxEdgeDistance)
        {
            RouteConnectionString = routeConnectionString;
            OsmConnectionString = osmConnectionString;
            Network = new RoutingNetwork(new GeometricGraph(1), maxEdgeDistance);
            EdgeProfiles = new AttributesIndex(AttributesIndexMode.IncreaseOne
                                               | AttributesIndexMode.ReverseAll);
            EdgeMeta = new AttributesIndex(AttributesIndexMode.ReverseStringIndexKeysOnly);
            VertexMeta = new MappedAttributesIndex();
            VertexData = new MetaCollectionDb();
            EdgeData = new MetaCollectionDb();
            Meta = new AttributeCollection();

            _supportedVehicles = new Dictionary<string, Vehicle>();
            _supportedProfiles = new Dictionary<string, Profile>();
            _contracted = new Dictionary<string, ContractedDb>();
            _restrictionDbs = new Dictionary<string, RestrictionsDb>();
            _shortcutsDbs = new Dictionary<string, ShortcutsDb>();

            Guid = guid;
        }

        /// <summary>
        ///     Creates a new router database.
        /// </summary>
        private RouterDb(Guid guid, RoutingNetwork network, AttributesIndex profiles, AttributesIndex meta,
            MappedAttributesIndex metaVertex,
            MetaCollectionDb vertexData, MetaCollectionDb edgeData, IAttributeCollection dbMeta,
            string routeConnectionString, string osmConnectionString, Vehicle[] supportedVehicles)
        {
            Guid = guid;
            Network = network;
            EdgeProfiles = profiles;
            EdgeMeta = meta;
            VertexMeta = metaVertex;
            VertexData = vertexData;
            EdgeData = edgeData;
            Meta = dbMeta;
            RouteConnectionString = routeConnectionString;
            OsmConnectionString = osmConnectionString;

            _supportedVehicles = new Dictionary<string, Vehicle>();
            _supportedProfiles = new Dictionary<string, Profile>();
            foreach (var vehicle in supportedVehicles)
            {
                _supportedVehicles[vehicle.Name.ToLowerInvariant()] = vehicle;
                foreach (var profile in vehicle.GetProfiles())
                    _supportedProfiles[profile.FullName.ToLowerInvariant()] = profile;
            }

            _contracted = new Dictionary<string, ContractedDb>();
            _restrictionDbs = new Dictionary<string, RestrictionsDb>();
            _shortcutsDbs = new Dictionary<string, ShortcutsDb>();
        }

        public Dictionary<long, long> NodeData { get; set; }

        public string RouteConnectionString { get; }
        public string OsmConnectionString { get; }

        /// <summary>
        ///     Returns the guid for this db.
        /// </summary>
        public Guid Guid { get; private set; }

        /// <summary>
        ///     Returns true if this router db is empty.
        /// </summary>
        public bool IsEmpty => Network.VertexCount == 0;

        /// <summary>
        ///     Returns the network.
        /// </summary>
        public RoutingNetwork Network { get; }

        /// <summary>
        ///     Gets all restriction dbs.
        /// </summary>
        public IEnumerable<RestrictionsDbMeta> RestrictionDbs
        {
            get
            {
                foreach (var kv in _restrictionDbs) yield return new RestrictionsDbMeta(kv.Key, kv.Value);
            }
        }

        /// <summary>
        ///     Returns the profiles index.
        /// </summary>
        public AttributesIndex EdgeProfiles { get; }

        /// <summary>
        ///     Returns the meta-data index.
        /// </summary>
        public AttributesIndex EdgeMeta { get; }

        /// <summary>
        ///     Gets or sets the vertex data.
        /// </summary>
        public MetaCollectionDb VertexData { get; }

        /// <summary>
        ///     Gets or sets the edge data.
        /// </summary>
        public MetaCollectionDb EdgeData { get; }

        /// <summary>
        ///     Returns the vertex meta-date index.
        /// </summary>
        public MappedAttributesIndex VertexMeta { get; }

        /// <summary>
        ///     Gets the meta-data collection.
        /// </summary>
        public IAttributeCollection Meta { get; }

        /// <summary>
        ///     Returns true if there is at least one contracted version of the network.
        /// </summary>
        public bool HasContracted => _contracted.Count > 0;

        /// <summary>
        ///     Returns true if there are shortcuts in this database.
        /// </summary>
        public bool HasShortcuts => _shortcutsDbs.Count > 0;

        /// <summary>
        ///     Returns true if there are restrictions in this database.
        /// </summary>
        public bool HasRestrictions => _restrictionDbs.Count > 0;

        public void LoadVertexes()
        {
            using var connection = new NpgsqlConnection(RouteConnectionString);
            connection.Open();
            using var command =
                new NpgsqlCommand(
                    @"SELECT id,latitude,longitude,ROW_NUMBER() OVER (ORDER BY id) FROM node WHERE guid=@guid AND is_core",
                    connection);
            command.Parameters.AddWithValue("guid", Guid);
            command.Prepare();
            var keyValuePairs = new List<KeyValuePair<long, long>>();
            using var reader = command.ExecuteReader();
            while (reader.Read())
            {
                var id = reader.GetInt64(0);
                var latitude = (float) reader.GetDouble(1);
                var longitude = (float) reader.GetDouble(2);
                var vertex = reader.GetInt64(3) - 1;
                Network.AddVertex(vertex, latitude, longitude);
                keyValuePairs.Add(new KeyValuePair<long, long>(id, vertex));
            }

            NodeData = keyValuePairs.ToDictionary(x => x.Key, x => x.Value);
        }

        public void LoadRestrictions()
        {
            using var connection = new NpgsqlConnection(RouteConnectionString);
            connection.Open();
            using var command =
                new NpgsqlCommand(
                    @"SELECT vehicle_type,nodes FROM restriction WHERE guid=@guid",
                    connection);
            command.Parameters.AddWithValue("guid", Guid);
            command.Prepare();
            using var reader = command.ExecuteReader();
            while (reader.Read())
            {
                var vehicleType = reader.GetString(0);
                var nodes = (long[]) reader.GetValue(1);
                if (!TryGetRestrictions(vehicleType, out var restrictions))
                {
                    restrictions = new RestrictionsDb();
                    AddRestrictions(vehicleType, restrictions);
                }

                restrictions.Add(nodes.Select(x => NodeData[x]).ToArray());
            }
        }

        public void LoadVertexMeta()
        {
            using var connection = new NpgsqlConnection(RouteConnectionString);
            connection.Open();
            connection.TypeMapper.MapComposite<RouteCoordinate>("coordinate");
            using var command =
                new NpgsqlCommand(
                    @"SELECT node,tags FROM meta WHERE guid=@guid",
                    connection);
            command.Parameters.AddWithValue("guid", Guid);
            command.Prepare();
            using var reader = command.ExecuteReader();
            while (reader.Read())
            {
                var node = reader.GetInt64(0);
                var tags = reader.GetValue(1) as Dictionary<string, string> ??
                                new Dictionary<string, string>();
                var attributes = new AttributeCollection();
                foreach (var (key, value) in tags) attributes.AddOrReplace(key, value);

                // add or set the attributes on the vertex.
                var existing = VertexMeta[NodeData[node]];
                if (existing != null)
                {
                    existing = new AttributeCollection(existing);
                    existing.AddOrReplace(attributes);
                }
                else
                {
                    existing = attributes;
                }

                VertexMeta[NodeData[node]] = existing;
            }
        }

        public void LoadEdges(bool Normalize = false)
        {
            using var connection = new NpgsqlConnection(RouteConnectionString);
            connection.Open();
            connection.TypeMapper.MapComposite<RouteCoordinate>("coordinate");
            using var command =
                new NpgsqlCommand(
                    @"SELECT from_node,to_node,distance,coordinates,meta_tags,profile_tags FROM edge WHERE guid=@guid",
                    connection);
            command.Parameters.AddWithValue("guid", Guid);
            command.Prepare();
            using var reader = command.ExecuteReader();
            while (reader.Read())
            {
                var from = reader.GetInt64(0);
                var to = reader.GetInt64(1);
                var distance = reader.GetDouble(2);
                var coordinates = reader.GetValue(3) as RouteCoordinate[] ?? new RouteCoordinate[0];
                var meta_tags = reader.GetValue(4) as Dictionary<string, string> ??
                                new Dictionary<string, string>();
                var profile_tags = reader.GetValue(5) as Dictionary<string, string> ??
                                   new Dictionary<string, string>();

                var metaTags = new AttributeCollection();
                var profileTags = new AttributeCollection();
                foreach (var (key, value) in meta_tags) metaTags.AddOrReplace(key, value);
                foreach (var (key, value) in profile_tags) profileTags.AddOrReplace(key, value);


                // get profile and meta-data id's.
                var profileCount = EdgeProfiles.Count;
                var profile = EdgeProfiles.Add(profileTags);
                if (profileCount != EdgeProfiles.Count)
                {
                    var stringBuilder = new StringBuilder();

                    if (!Normalize)
                    {
                        // no normalization, translate profiles by.
                        metaTags.AddOrReplace(profileTags);

                        // translate profile.
                        var translatedProfile = new AttributeCollection();
                        translatedProfile.AddOrReplace("translated_profile", "yes");
                        foreach (var vehicleProfile in GetSupportedProfiles())
                        {
                            var factorAndSpeed = vehicleProfile.FactorAndSpeed(profileTags);
                            translatedProfile.AddOrReplace($"{vehicleProfile.FullName}",
                                $"{factorAndSpeed.Direction}|" +
                                factorAndSpeed.Value.ToInvariantString() + "|" +
                                factorAndSpeed.SpeedFactor.ToInvariantString());
                        }

                        var translatedCount = EdgeProfiles.Count;
                        var translatedProfileId = EdgeProfiles.Add(translatedProfile);
                        if (translatedProfileId > EdgeDataSerializer.MAX_PROFILE_COUNT)
                            throw new Exception(
                                "Maximum supported profiles exceeded, make sure only routing tags are included in the profiles.");
                        _translatedPerOriginal[profile] = (ushort) translatedProfileId;
                        profile = translatedProfileId;
                        if (translatedCount != EdgeProfiles.Count)
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
                                "New translated profile: # {0}: {1}", EdgeProfiles.Count,
                                stringBuilder.ToInvariantString());
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
                            "New edge profile: # profiles {0}: {1}", EdgeProfiles.Count,
                            stringBuilder.ToInvariantString());

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


                var meta = EdgeMeta.Add(metaTags);
                var fromVertex = NodeData[from];
                var toVertex = NodeData[to];
                var intermediates = coordinates.Select(x => new Coordinate(x.Latitude, x.Longitude))
                    .ToList();
                AddCoreEdge(fromVertex, toVertex, new EdgeData
                {
                    MetaId = meta,
                    Distance = (float) distance,
                    Profile = (ushort) profile
                }, intermediates);
            }
        }

        /// <summary>
        ///     Adds a new edge.
        /// </summary>
        public void AddCoreEdge(long vertex1, long vertex2, EdgeData data, List<Coordinate> shape,
            float simplifyEpsilonInMeter = .1f)
        {
            if (data.Distance >= Network.MaxEdgeDistance)
                // distance is too big to fit into the graph's data field.
                // just add the edge with the max distance, length can be recalculated on the fly for these edges 
                // or (and this is what's probably done) we split up the edge later and add a proper length.
                data = new EdgeData
                {
                    Distance = Network.MaxEdgeDistance,
                    Profile = data.Profile,
                    MetaId = data.MetaId
                };

            // add the edge.
            var edgeId = Network.AddEdge(vertex1, vertex2, data,
                shape.Simplify(simplifyEpsilonInMeter));
        }

        /// <summary>
        ///     Generates a new guid.
        /// </summary>
        /// <remarks>To use then the network was changed externally and was already writting to disk before.</remarks>
        public void NewGuid()
        {
            Guid = Guid.NewGuid();
        }

        /// <summary>
        ///     Returns true if the given vehicle is supported.
        /// </summary>
        public bool Supports(string vehicleName)
        {
            return _supportedVehicles.ContainsKey(vehicleName.ToLowerInvariant());
        }

        /// <summary>
        ///     Gets one of the supported vehicles.
        /// </summary>
        public Vehicle GetSupportedVehicle(string vehicleName)
        {
            return _supportedVehicles[vehicleName.ToLowerInvariant()];
        }

        /// <summary>
        ///     Gets all the supported vehicle.
        /// </summary>
        /// <returns></returns>
        public IEnumerable<Vehicle> GetSupportedVehicles()
        {
            return _supportedVehicles.Values;
        }

        /// <summary>
        ///     Adds a supported vehicle.
        /// </summary>
        public void AddSupportedVehicle(Vehicle vehicle)
        {
            _supportedVehicles[vehicle.Name.ToLowerInvariant()] = vehicle;
            foreach (var profile in vehicle.GetProfiles())
                _supportedProfiles[profile.FullName.ToLowerInvariant()] = profile;
        }

        /// <summary>
        ///     Returns true if the profile with the given name is supported.
        /// </summary>
        public bool SupportProfile(string profileName)
        {
            return _supportedProfiles.ContainsKey(profileName);
        }

        /// <summary>
        ///     Gets one of the supported vehicles.
        /// </summary>
        public Profile GetSupportedProfile(string profileName)
        {
            return _supportedProfiles[profileName.ToLowerInvariant()];
        }

        /// <summary>
        ///     Gets one of the supported profiles.
        /// </summary>
        public IEnumerable<Profile> GetSupportedProfiles()
        {
            foreach (var vehicle in GetSupportedVehicles())
            foreach (var profile in vehicle.GetProfiles())
                yield return profile;
        }

        /// <summary>
        ///     Adds a contracted version of the routing network for the given profile.
        /// </summary>
        public void AddContracted(Profile profile, ContractedDb contracted)
        {
            _contracted[profile.FullName] = contracted;
        }

        /// <summary>
        ///     Removes the contracted version of the routing network for the given profile.
        /// </summary>
        public bool RemoveContracted(Profile profile)
        {
            return _contracted.Remove(profile.FullName);
        }

        /// <summary>
        ///     Tries to get a contracted version of the routing network for the given profile.
        /// </summary>
        public bool TryGetContracted(Profile profile, out ContractedDb contracted)
        {
            return _contracted.TryGetValue(profile.FullName, out contracted);
        }

        /// <summary>
        ///     Gets all the profiles that have contracted db's.
        /// </summary>
        /// <returns></returns>
        public IEnumerable<string> GetContractedProfiles()
        {
            return _contracted.Select(x => x.Key);
        }

        /// <summary>
        ///     Returns true if this routing db has a contracted version of the routing network for the given profile.
        /// </summary>
        public bool HasContractedFor(Profile profile)
        {
            return _contracted.ContainsKey(profile.FullName);
        }

        /// <summary>
        ///     Adds a shortcuts db.
        /// </summary>
        public void AddShortcuts(string name, ShortcutsDb shortcutsDb)
        {
            _shortcutsDbs[name] = shortcutsDb;
        }

        /// <summary>
        ///     Removes a shortcuts db.
        /// </summary>
        public bool RemoveShortcuts(string name)
        {
            return _shortcutsDbs.Remove(name);
        }

        /// <summary>
        ///     Tries to get a shortcuts db.
        /// </summary>
        public bool TryGetShortcuts(string name, out ShortcutsDb shortcutsDb)
        {
            return _shortcutsDbs.TryGetValue(name, out shortcutsDb);
        }

        /// <summary>
        ///     Gets the names of the restricted vehicle types.
        /// </summary>
        /// <returns></returns>
        public IEnumerable<string> GetRestrictedVehicleTypes()
        {
            return _restrictionDbs.Select(x => x.Key);
        }

        /// <summary>
        ///     Returns true if this routing db has a restriction db for the given vehicle type.
        /// </summary>
        public bool TryGetRestrictions(string vehicleType, out RestrictionsDb restrictions)
        {
            return _restrictionDbs.TryGetValue(vehicleType, out restrictions);
        }

        /// <summary>
        ///     Adds the restrictions for the given vehicle type.
        /// </summary>
        public void AddRestrictions(string vehicleType, RestrictionsDb restrictions)
        {
            _restrictionDbs[vehicleType] = restrictions;
        }

        /// <summary>
        ///     Removes the restrictions for the given vehicle type.
        /// </summary>
        public bool RemoveRestrictions(string vehicleType)
        {
            return _restrictionDbs.Remove(vehicleType);
        }

        /// <summary>
        ///     Compresses the network and rearranges all id's as needed.
        /// </summary>
        public void Compress()
        {
            Network.Compress((originalId, newId) =>
            {
                if (EdgeData != null) EdgeData.Switch(originalId, newId);
            });
        }

        /// <summary>
        ///     Writes this database to the given stream.
        /// </summary>
        public long Serialize(Stream stream)
        {
            return Serialize(stream, true);
        }

        /// <summary>
        ///     Writes this database to the given stream.
        /// </summary>
        public long Serialize(Stream stream, bool toReadonly)
        {
            var position = stream.Position;

            // write version #.
            // version1: OsmSharp.Routing state of layout.
            // version2: Added ShortcutsDbs.
            // version3: Add advanced profile serialization.
            // version4: Added missing restriction dbs.
            // version5: Added new dual edge-based contracted graph.
            // version6: Added vertex meta-data.
            // version7: Added support for shorts in vertex meta-data.
            // version8: Added edge meta-data.
            // version9: Writable attribute indexes.
            long size = 1;
            stream.WriteByte(9);

            // write guid.
            stream.Write(Guid.ToByteArray(), 0, 16);
            size += 16;

            // serialize supported profiles.
            var lengthBytes = BitConverter.GetBytes(_supportedVehicles.Count);
            size += 4;
            stream.Write(lengthBytes, 0, 4);
            foreach (var vehicle in _supportedVehicles) size += vehicle.Value.Serialize(stream);

            // serialize the db-meta.
            size += Meta.WriteWithSize(stream);

            // serialize the # of shortcutsdbs profiles.
            if (_shortcutsDbs.Count > byte.MaxValue)
                throw new Exception("Cannot serialize a router db with more than 255 shortcut dbs.");
            stream.WriteByte((byte) _shortcutsDbs.Count);
            size += 1;

            // serialize the # of contracted profiles.
            if (_contracted.Count > byte.MaxValue)
                throw new Exception("Cannot serialize a router db with more than 255 contracted graphs.");
            stream.WriteByte((byte) _contracted.Count);
            size += 1;

            // serialize the # of restriction dbs.
            if (_restrictionDbs.Count > byte.MaxValue)
                throw new Exception("Cannot serialize a router db with more than 255 restriction dbs.");
            stream.WriteByte((byte) _restrictionDbs.Count);
            size += 1;

            // serialize edge profiles.
            size += EdgeProfiles.Serialize(new LimitedStream(stream));
            stream.Seek(position + size, SeekOrigin.Begin);

            // serialize meta-data.
            size += EdgeMeta.Serialize(new LimitedStream(stream));
            stream.Seek(position + size, SeekOrigin.Begin);

            // serialize vertex meta-data.
            size += VertexMeta.Serialize(new LimitedStream(stream));
            stream.Seek(position + size, SeekOrigin.Begin);

            // serialize vertex data.
            size += VertexData.Serialize(stream);
            stream.Seek(position + size, SeekOrigin.Begin);

            // serialize edge data.
            size += EdgeData.Serialize(stream);
            stream.Seek(position + size, SeekOrigin.Begin);

            // serialize network.
            size += Network.Serialize(new LimitedStream(stream), (originalId, newId) =>
            {
                if (EdgeData != null) EdgeData.Switch(originalId, newId);
            });
            stream.Seek(position + size, SeekOrigin.Begin);

            // serialize all shortcut dbs.
            foreach (var shortcutsDb in _shortcutsDbs)
            {
                size += stream.WriteWithSize(shortcutsDb.Key);
                size += shortcutsDb.Value.Serialize(
                    new LimitedStream(stream));
            }

            // serialize all contracted networks.
            foreach (var contracted in _contracted)
            {
                size += stream.WriteWithSize(contracted.Key);
                size += contracted.Value.Serialize(
                    new LimitedStream(stream), toReadonly);
            }

            // serialize all restriction dbs.
            foreach (var restrictionDb in _restrictionDbs)
            {
                size += stream.WriteWithSize(restrictionDb.Key);
                size += restrictionDb.Value.Serialize(stream);
            }

            return size;
        }

        /// <summary>
        ///     Writes the contracted graph for the given profile to the given stream.
        /// </summary>
        public long SerializeContracted(Profile profile, Stream stream)
        {
            ContractedDb contracted;
            if (!TryGetContracted(profile, out contracted))
                throw new Exception(string.Format("Contracted graph for profile {0} not found.", profile.FullName));

            // write: guid, name and data.

            var guid = Guid;
            long size = 16;
            stream.Write(guid.ToByteArray(), 0, 16);
            size += stream.WriteWithSize(profile.FullName);
            size += contracted.Serialize(stream, true);
            return size;
        }

        /// <summary>
        ///     Reads a contracted graph from the given stream and adds it to this db.
        /// </summary>
        public void DeserializeAndAddContracted(Stream stream)
        {
            DeserializeAndAddContracted(stream, null);
        }

        /// <summary>
        ///     Reads a contracted graph from the given stream and adds it to this db.
        /// </summary>
        public void DeserializeAndAddContracted(Stream stream, ContractedDbProfile profile)
        {
            // first read and compare guids.
            var guidBytes = new byte[16];
            stream.Read(guidBytes, 0, 16);
            var guid = new Guid(guidBytes);
            if (guid != Guid) throw new Exception("Cannot add this contracted graph, guid's do not match.");
            var profileName = stream.ReadWithSizeString();
            var contracted = ContractedDb.Deserialize(stream, profile);
            _contracted[profileName] = contracted;
        }

        /// <summary>
        ///     Deserializes a database from the given stream.
        /// </summary>
        public static RouterDb Deserialize(Stream stream)
        {
            return Deserialize(stream, null);
        }

        /// <summary>
        ///     Deserializes a database from the given stream.
        /// </summary>
        public static RouterDb Deserialize(Stream stream, RouterDbProfile profile)
        {
            // deserialize all basic data.
            // version1: OsmSharp.Routing state of layout.
            // version2: Added ShortcutsDbs.
            // version3: Add advanced profile serialization.
            // version4: Added missing restriction dbs.
            // version5: Added new dual edge-based contracted graph.
            // version6: Added vertex meta-data.
            // version7: Added support for shorts in vertex meta-data.
            // version8: Added edge meta-data.
            // version9: Writable attribute indexes.
            var version = stream.ReadByte();
            if (version != 1 && version != 2 && version != 3 && version != 4 && version != 5 && version != 6 &&
                version != 7 && version != 8 && version != 9)
                throw new Exception(string.Format("Cannot deserialize routing db: Invalid version #: {0}.", version));

            var guidBytes = new byte[16];
            stream.Read(guidBytes, 0, 16);
            var guid = new Guid(guidBytes);

            var supportedVehicleInstances = new List<Vehicle>();
            if (version <= 2)
            {
                // just contains vehicle names.
                var supportedVehicles = stream.ReadWithSizeStringArray();
                foreach (var vehicleName in supportedVehicles)
                    if (Profile.TryGet(vehicleName, out var vehicleProfile))
                        supportedVehicleInstances.Add(vehicleProfile.Parent);
                    else
                        Logger.Log("RouterDb", TraceEventType.Warning,
                            "Vehicle with name {0} was not found, register all vehicle profiles before deserializing the router db.",
                            vehicleName);
            }
            else
            {
                // contains the full vehicles.
                var lengthBytes = new byte[4];
                stream.Read(lengthBytes, 0, 4);
                var size = BitConverter.ToInt32(lengthBytes, 0);
                for (var i = 0; i < size; i++)
                {
                    var vehicle = Vehicle.Deserialize(stream);
                    supportedVehicleInstances.Add(vehicle);
                }
            }

            var metaDb = stream.ReadWithSizeAttributesCollection();
            var shorcutsCount = 0;
            if (version >= 2)
                // when version < 1 there are no shortcuts and thus no shortcut count.
                shorcutsCount = stream.ReadByte();
            var contractedCount = stream.ReadByte();

            var restrictionDbCount = 0;
            if (version >= 4) restrictionDbCount = stream.ReadByte();

            var profiles = AttributesIndex.Deserialize(new LimitedStream(stream), true);
            var meta = AttributesIndex.Deserialize(new LimitedStream(stream), true);
            MappedAttributesIndex metaVertex = null;
            MetaCollectionDb vertexData = null;
            if (version >= 6)
            {
                metaVertex = MappedAttributesIndex.Deserialize(new LimitedStream(stream),
                    profile == null ? null : profile.VertexMetaProfile);
                vertexData = MetaCollectionDb.Deserialize(new LimitedStream(stream),
                    profile == null ? null : profile.VertexDataProfile);
            }

            MetaCollectionDb edgeData = null;
            if (version >= 8)
                edgeData = MetaCollectionDb.Deserialize(new LimitedStream(stream),
                    profile == null ? null : profile.VertexDataProfile);
            var network = RoutingNetwork.Deserialize(stream, profile == null ? null : profile.RoutingNetworkProfile);

            // create router db.
            var routerDb = new RouterDb(guid, network, profiles, meta, metaVertex, vertexData, edgeData, metaDb, null,
                null, supportedVehicleInstances.ToArray());

            // read all shortcut dbs.
            for (var i = 0; i < shorcutsCount; i++)
            {
                var shortcutsName = stream.ReadWithSizeString();
                var shorcutsDb = ShortcutsDb.Deserialize(stream);
                routerDb._shortcutsDbs[shortcutsName] = shorcutsDb;
            }

            // read all contracted versions.
            for (var i = 0; i < contractedCount; i++)
            {
                var profileName = stream.ReadWithSizeString();
                var contracted = ContractedDb.Deserialize(stream, profile == null ? null : profile.ContractedDbProfile);
                routerDb._contracted[profileName] = contracted;
            }

            // read all restriction dbs.
            for (var i = 0; i < restrictionDbCount; i++)
            {
                var restrictionDbName = stream.ReadWithSizeString();
                var restrictionDb =
                    RestrictionsDb.Deserialize(stream, profile == null ? null : profile.RestrictionDbProfile);
                routerDb._restrictionDbs[restrictionDbName] = restrictionDb;
            }

            return routerDb;
        }
    }
}