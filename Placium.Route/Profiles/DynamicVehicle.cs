using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Route.Attributes;
using Route.Profiles.Lua;
using Route.Profiles.Lua.DataTypes;

namespace Placium.Route.Profiles
{
    /// <summary>
    ///     A dynamic vehicle based on single lua script.
    /// </summary>
    public class DynamicVehicle : Vehicle
    {
        private readonly HashSet<string> _metaWhiteList;

        private readonly Dictionary<string, object> _profileFunctions;
        private readonly HashSet<string> _profileWhiteList;
        private readonly string _source;

        private Table _attributesTable;
        private Table _resultsTable;

        /// <summary>
        ///     Creates a new dynamic profile based on the given lua script.
        /// </summary>
        public DynamicVehicle(string script)
        {
            _profileFunctions = new Dictionary<string, object>();
            _source = script;

            Script = new Script();
            Script.DoString(script);

            var dynName = Script.Globals.Get("name");
            if (dynName == null) throw new Exception("Dynamic profile doesn't define a name.");
            Name = dynName.String;

            var dynNormalize = Script.Globals.Get("normalize");
            if (dynNormalize == null ||
                DynValue.Nil.Equals(dynNormalize) ||
                DynValue.True.Equals(dynNormalize))
                Normalize = true;
            else
                Normalize = dynNormalize.Boolean;

            var dynVehicleTypes = Script.Globals.Get("vehicle_types");
            if (dynVehicleTypes != null &&
                dynVehicleTypes.Type == DataType.Table)
                VehicleTypes = dynVehicleTypes.Table.Values.Select(x => x.String).ToArray();
            else
                VehicleTypes = new string[0];

            var dynProfiles = Script.Globals.Get("profiles");
            if (dynProfiles == null) throw new ArgumentException("No profiles defined in lua script.");
            foreach (var dynProfile in dynProfiles.Table.Pairs)
            {
                var profileDefinition = dynProfile.Value;
                var profileName = profileDefinition.Table.Get("name").String;
                var functionName = profileDefinition.Table.Get("function_name").String;
                var function = Script.Globals[functionName];
                if (function == null)
                    throw new ArgumentException(string.Format("Function {0} not found in lua script.", functionName));

                var metric = ProfileMetric.Custom;
                var dynMetric = profileDefinition.Table.Get("metric");
                if (dynMetric != null)
                    switch (dynMetric.String)
                    {
                        case "time":
                            metric = ProfileMetric.TimeInSeconds;
                            break;
                        case "distance":
                            metric = ProfileMetric.DistanceInMeters;
                            break;
                    }

                if (!_profileFunctions.ContainsKey(functionName)) _profileFunctions[functionName] = function;
                var profile = new DynamicProfile(profileName, metric, VehicleTypes, this, Script, function);
                Register(profile);
            }

            var dynAttributesWhitelist = Script.Globals.Get("meta_whitelist");
            _metaWhiteList = new HashSet<string>();
            if (dynAttributesWhitelist != null)
                foreach (var attribute in dynAttributesWhitelist.Table.Values.Select(x => x.String))
                    _metaWhiteList.Add(attribute);

            dynAttributesWhitelist = Script.Globals.Get("profile_whitelist");
            _profileWhiteList = new HashSet<string>();
            if (dynAttributesWhitelist != null)
                foreach (var attribute in dynAttributesWhitelist.Table.Values.Select(x => x.String))
                    _profileWhiteList.Add(attribute);

            var dynParameters = Script.Globals.Get("parameters");
            var parameters = new AttributeCollection();
            if (dynParameters != null && dynParameters.Table != null)
                foreach (var dynParameter in dynParameters.Table.Pairs)
                {
                    var parameterName = dynParameter.Key;
                    var parameterValue = dynParameter.Value;

                    parameters.AddOrReplace(parameterName.String, parameterValue.String);
                }

            Parameters = parameters;
        }

        /// <summary>
        ///     Gets all the profile functions.
        /// </summary>
        protected IEnumerable<object> ProfileFunctions => _profileFunctions.Values;

        /// <summary>
        ///     Gets the script.
        /// </summary>
        public Script Script { get; }

        /// <summary>
        ///     Gets the name.
        /// </summary>
        public sealed override string Name { get; }

        /// <summary>
        ///     Gets the normalize flag.
        /// </summary>
        public override bool Normalize { get; }

        /// <summary>
        ///     Gets the vehicle types.
        /// </summary>
        public sealed override string[] VehicleTypes { get; }

        /// <summary>
        ///     Gets the attributes whitelist.
        /// </summary>
        public override HashSet<string> MetaWhiteList => _metaWhiteList;

        /// <summary>
        ///     Gets the attributes whitelist.
        /// </summary>
        public override HashSet<string> ProfileWhiteList => _profileWhiteList;

        /// <summary>
        ///     Gets the parameters.
        /// </summary>
        public override IReadonlyAttributeCollection Parameters { get; }

        /// <summary>
        /// </summary>
        public override bool AddToWhiteList(IAttributeCollection attributes, Whitelist whitelist)
        {
            if (_attributesTable == null)
            {
                _attributesTable = new Table(Script);
                _resultsTable = new Table(Script);
            }

            var traversable = false;

            // build lua table.
            _attributesTable.Clear();
            foreach (var attribute in attributes)
                _attributesTable.Set(attribute.Key, DynValue.NewString(attribute.Value));

            // call each function once and build the list of attributes to keep.
            foreach (var function in ProfileFunctions)
            {
                // call factor_and_speed function.
                _resultsTable.Clear();
                Script.Call(function, _attributesTable, _resultsTable);

                if (_resultsTable.TryGetFloat("speed", out var val))
                    if (val != 0)
                        traversable = true;

                // get the result.
                var dynAttributesToKeep = _resultsTable.Get("attributes_to_keep");
                if (dynAttributesToKeep == null) continue;
                foreach (var attribute in dynAttributesToKeep.Table.Keys.Select(x => x.String))
                    whitelist.Add(attribute);
            }

            return traversable;
        }

        /// <summary>
        ///     Pushes the attributes through this profiles and adds used keys in the given whitelist.
        /// </summary>
        public override FactorAndSpeed FactorAndSpeed(IAttributeCollection attributes, Whitelist whiteList)
        {
            throw new NotImplementedException("Not used and unavailable with dynamic vehicles.");
        }

        /// <summary>
        ///     Loads the vehicle from the given script.
        /// </summary>
        public static DynamicVehicle Load(string script)
        {
            var vehicle = new DynamicVehicle(script);
            return vehicle;
        }

        /// <summary>
        ///     Loads the vehicle from the given stream using the current position as the size.
        /// </summary>
        public static DynamicVehicle LoadWithSize(Stream stream)
        {
            return Load(stream.ReadWithSizeString());
        }
    }
}