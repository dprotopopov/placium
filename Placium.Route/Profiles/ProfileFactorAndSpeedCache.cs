using System;
using System.Collections.Generic;
using System.Linq;

namespace Placium.Route.Profiles
{
    /// <summary>
    /// A profile factor and speed cache.
    /// </summary>
    public class ProfileFactorAndSpeedCache
    {
        private readonly RouterDb _db;
        private Dictionary<string, FactorAndSpeed[]> _edgeProfileFactors;

        /// <summary>
        /// A profile factor cache.
        /// </summary>
        public ProfileFactorAndSpeedCache(RouterDb db)
        {
            _db = db;
            _edgeProfileFactors = new Dictionary<string, FactorAndSpeed[]>();
        }

        /// <summary>
        /// Gets the router db.
        /// </summary>
        public RouterDb RouterDb
        {
            get
            {
                return _db;
            }
        }

        /// <summary>
        /// Returns true if all the given profiles are cached and supported.
        /// </summary>
        public bool ContainsAll(params Profile[] profiles)
        {
            for (var p = 0; p < profiles.Length; p++)
            {
                if (!_edgeProfileFactors.ContainsKey(profiles[p].FullName))
                {
                    return false;
                }
            }
            return true;
        }

        /// <summary>
        /// Returns true if all the given profiles are cached and supported.
        /// </summary>
        public bool ContainsAll(params IProfileInstance[] profileInstances)
        {
            for (var p = 0; p < profileInstances.Length; p++)
            {
                if (!_edgeProfileFactors.ContainsKey(profileInstances[p].Profile.FullName))
                {
                    return false;
                }
            }
            return true;
        }

        /// <summary>
        /// Calculates for all registered profiles.
        /// </summary>
        [Obsolete]
        public void CalculateForAll()
        {
            this.CalculateFor(Profile.GetRegistered().ToArray());
        }

        /// <summary>
        /// Precalculates speed factors for all the given profiles.
        /// </summary>
        public void CalculateFor(params Profile[] profiles)
        {
            lock (this)
            { // don't allow multiple threads to fill this cache at the same time.
                var newEdgeProfileFactors = new Dictionary<string, FactorAndSpeed[]>(_edgeProfileFactors);

                var edgeProfileFactors = new FactorAndSpeed[profiles.Length][];
                for (var p = 0; p < profiles.Length; p++)
                {
                    edgeProfileFactors[p] = new FactorAndSpeed[(int) _db.EdgeProfiles.Count];
                }

                for (long edgeProfile = 0; edgeProfile < _db.EdgeProfiles.Count; edgeProfile++)
                {
                    var edgeProfileTags = _db.EdgeProfiles.Get(edgeProfile);
                    for (var p = 0; p < profiles.Length; p++)
                    {
                        edgeProfileFactors[p][edgeProfile]
                            = profiles[p].FactorAndSpeed(edgeProfileTags);
                    }
                }

                for (var p = 0; p < profiles.Length; p++)
                {
                    newEdgeProfileFactors[profiles[p].FullName] = edgeProfileFactors[p];
                }

                _edgeProfileFactors = newEdgeProfileFactors;
            }
        }


        /// <summary>
        /// Gets the get factor function for the given profile.
        /// </summary>
        public Func<ushort, Factor> GetGetFactor(IProfileInstance profileInstance)
        {
            if (!_edgeProfileFactors.TryGetValue(profileInstance.Profile.FullName, out var cache)) throw new ArgumentException("Given profile not supported.");
            var cachedFactors = cache;
            if (profileInstance.Constraints != null)
            {
                return (p) =>
                {
                    var cachedFactor = cachedFactors[p];
                    if (profileInstance.IsConstrained(cachedFactor.Constraints))
                    {
                        return Factor.NoFactor;
                    }
                    return cachedFactor.ToFactor();
                };
            }
            return (p) =>
            {
                return cachedFactors[p].ToFactor();
            };
        }

        /// <summary>
        /// Gets the get factor function for the given profile.
        /// </summary>
        public Func<ushort, FactorAndSpeed> GetGetFactorAndSpeed(IProfileInstance profileInstance)
        {
            if (!_edgeProfileFactors.TryGetValue(profileInstance.Profile.FullName, out var cache)) throw new ArgumentException("Given profile not supported.");
            var cachedFactors = cache;
            if (profileInstance.Constraints != null)
            {
                return (p) =>
                {
                    var cachedFactor = cachedFactors[p];
                    if (profileInstance.IsConstrained(cachedFactor.Constraints))
                    {
                        return FactorAndSpeed.NoFactor;
                    }
                    return cachedFactor;
                };
            }
            return (p) =>
            {
                return cachedFactors[p];
            };
        }

        /// <summary>
        /// Returns the cached factor.
        /// </summary>
        public Factor GetFactor(ushort edgeProfile, string profileName)
        {
            FactorAndSpeed[] factorsForProfile;
            if (!_edgeProfileFactors.TryGetValue(profileName, out factorsForProfile))
            {
                throw new ArgumentOutOfRangeException(string.Format("{0} not found.", profileName));
            }
            if (edgeProfile < factorsForProfile.Length)
            {
                return factorsForProfile[edgeProfile].ToFactor();
            }
            throw new ArgumentOutOfRangeException("Edgeprofile invalid.");
        }

        /// <summary>
        /// Returns the cached factor and speed.
        /// </summary>
        public FactorAndSpeed GetFactorAndSpeed(ushort edgeProfile, string profileName)
        {
            FactorAndSpeed[] factorsForProfile;
            if (!_edgeProfileFactors.TryGetValue(profileName, out factorsForProfile))
            {
                throw new ArgumentOutOfRangeException(string.Format("{0} not found.", profileName));
            }
            if (edgeProfile < factorsForProfile.Length)
            {
                return factorsForProfile[edgeProfile];
            }
            throw new ArgumentOutOfRangeException("Edgeprofile invalid.");
        }

        /// <summary>
        /// Returns true if the given edge can be stopped on.
        /// </summary>
        public bool CanStopOn(ushort edgeProfile, string profileName)
        {
            FactorAndSpeed[] factorsForProfile;
            if (!_edgeProfileFactors.TryGetValue(profileName, out factorsForProfile))
            {
                throw new ArgumentOutOfRangeException(string.Format("{0} not found.", profileName));
            }
            if (edgeProfile < factorsForProfile.Length)
            {
                return factorsForProfile[edgeProfile].CanStopOn();
            }
            throw new ArgumentOutOfRangeException("Edgeprofile invalid.");
        }
    }
}
