using System.Collections.Generic;
using System.Globalization;
using OsmSharp.Tags;
using Placium.Route.Profiles;

namespace Placium.Route.Osm.Vehicles
{
    public class ResultFactorAndSpeed
    {
        public float Factor { get; set; }
        public float Speed { get; set; }
        public short Direction { get; set; }
        public bool CanStop { get; set; }
    }

    public class ResultNodeRestriction
    {
        public string Vehicle { get; set; }
    }

    public class Car
    {
        private readonly Dictionary<string, bool> _accessValues = new Dictionary<string, bool>
        {
            {"private", false},
            {"yes", true},
            {"no", false},
            {"permissive", true},
            {"destination", true},
            {"customers", false},
            {"designated", true},
            {"public", true},
            {"delivery", true},
            {"use_sidepath", false}
        };

        private readonly Dictionary<string, bool> _barriers = new Dictionary<string, bool>
        {
            {"gate", true},
            {"bollard", true},
            {"fence", true}
        };

        private readonly Dictionary<string, float> _classificationsFactors = new Dictionary<string, float>
        {
            {"motorway", 10f},
            {"motorway_link", 10f},
            {"trunk", 9f},
            {"trunk_link", 9f},
            {"primary", 8f},
            {"primary_link", 8f},
            {"secondary", 7f},
            {"secondary_link", 7f},
            {"tertiary", 6f},
            {"tertiary_link", 6f},
            {"unclassified", 5f},
            {"residential", 5f}
        };

        private readonly List<string> _constraints = new List<string> {"maxweight", "maxwidth"};

        private readonly float _maxSpeed = 200f;

        private readonly float _minSpeed = 30f;

        private readonly Dictionary<string, float> _speedProfile = new Dictionary<string, float>
        {
            {"motorway", 120f},
            {"motorway_link", 120f},
            {"trunk", 90f},
            {"trunk_link", 90f},
            {"primary", 90f},
            {"primary_link", 90f},
            {"secondary", 70f},
            {"secondary_link", 70f},
            {"tertiary", 70f},
            {"tertiary_link", 70f},
            {"unclassified", 50f},
            {"residential", 50f},
            {"service", 30f},
            {"services", 30f},
            {"road", 30f},
            {"track", 30f},
            {"living_street", 5f},
            {"ferry", 5f},
            {"movable", 5f},
            {"shuttle_train", 10f},
            {"default", 10f}
        };

        public string[] VehicleTypes => new[] {"vehicle", "motor_vehicle", "motorcar"};

        public string Name => nameof(Car);

        public string[] GetProfiles => new[]
            {$"{nameof(Car)}", $"{nameof(Car)}.Shortest", $"{nameof(Car)}.Classifications"};

        public bool CanTraverse(TagsCollectionBase attributes)
        {
            if (!CanAccess(attributes)) return false;

            attributes.TryGetValue("highway", out var highway);
            attributes.TryGetValue("route", out var route);
            if (route == "ferry") highway = "ferry";
            if (string.IsNullOrEmpty(highway) || !_speedProfile.TryGetValue(highway, out var highwaySpeed))
                return false;
            return highwaySpeed > 0;
        }

        public Dictionary<string, FactorAndSpeed> FactorAndSpeeds(TagsCollectionBase attributes)
        {
            var factorAndspeeds = new Dictionary<string, FactorAndSpeed>();
            var result = FactorAndSpeed(attributes);
            var classifications = FactorAndSpeedClassifications(attributes, result);
            if (result != null)
                factorAndspeeds.Add($"{nameof(Car)}", new FactorAndSpeed
                {
                    Factor = 1f / (result.Speed / 3.6f),
                    Direction = (short) (result.Direction + (!result.CanStop ? 3 : 0))
                });
            if (result != null)
                factorAndspeeds.Add($"{nameof(Car)}.Shortest", new FactorAndSpeed
                {
                    Factor = result.Factor,
                    Direction = (short) (result.Direction + (!result.CanStop ? 3 : 0))
                });
            if (classifications != null)
                factorAndspeeds.Add($"{nameof(Car)}.Classifications", new FactorAndSpeed
                {
                    Factor = classifications.Factor,
                    Direction = (short) (classifications.Direction + (!classifications.CanStop ? 3 : 0))
                });
            return factorAndspeeds;
        }

        public bool NodeTagProcessor(TagsCollectionBase attributes)
        {
            return false;
        }

        public ResultNodeRestriction NodeRestriction(TagsCollectionBase attributes)
        {
            if (!attributes.TryGetValue("barrier", out var barrier) || string.IsNullOrEmpty(barrier)) return null;
            if (!_barriers.TryGetValue(barrier, out var value) || !value) return null;
            return new ResultNodeRestriction {Vehicle = "motorcar"};
        }

        private short? IsOneway(TagsCollectionBase attributes, string name)
        {
            if (!attributes.TryGetValue(name, out var oneway)) return null;
            if (oneway == "yes" || oneway == "true" || oneway == "1")
                return 1;
            if (oneway == "-1")
                return 2;
            return null;
        }

        private bool CanAccess(TagsCollectionBase attributes)
        {
            var lastAccess = true;
            if (attributes.TryGetValue("access", out var accessKey) &&
                _accessValues.TryGetValue(accessKey, out var access))
                lastAccess = access;
            foreach (var vehicleType in VehicleTypes)
                if (attributes.TryGetValue(vehicleType, out accessKey) &&
                    _accessValues.TryGetValue(accessKey, out access))
                    lastAccess = access;
            return lastAccess;
        }

        private ResultFactorAndSpeed FactorAndSpeed(TagsCollectionBase attributes)
        {
            attributes.TryGetValue("highway", out var highway);
            attributes.TryGetValue("route", out var route);
            if (route == "ferry") highway = "ferry";
            if (string.IsNullOrEmpty(highway) || !_speedProfile.TryGetValue(highway, out var highwaySpeed)) return null;

            var result = new ResultFactorAndSpeed();
            result.Factor = 1f;
            result.Speed = highwaySpeed;
            result.Direction = 0;
            result.CanStop = true;
            if (highway == "motorway" ||
                highway == "motorway_link")
                result.CanStop = false;

            if (attributes.TryGetValue("maxspeed", out var maxSpeed) && float.TryParse(maxSpeed, NumberStyles.Any,
                    CultureInfo.InvariantCulture, out var speed))
                result.Speed = 0.75f * speed;

            if (attributes.TryGetValue("junction", out var junction) && junction == "roundabout")
                result.Direction = 1;
            var direction = IsOneway(attributes, "oneway");
            if (direction != null) result.Direction = direction.Value;

            return result;
        }

        private ResultFactorAndSpeed FactorAndSpeedClassifications(TagsCollectionBase attributes,
            ResultFactorAndSpeed source)
        {
            if (source == null) return null;
            var result = new ResultFactorAndSpeed
            {
                Factor = 1f / (source.Speed / 3.6f),
                Speed = source.Speed,
                Direction = source.Direction,
                CanStop = source.CanStop
            };
            if (attributes.TryGetValue("highway", out var highway) &&
                _classificationsFactors.TryGetValue(highway, out var classificationFactor))
                result.Factor /= classificationFactor;
            else result.Factor /= 4f;
            return result;
        }
    }
}