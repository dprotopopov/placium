using System;
using Route.Attributes;

namespace Placium.Route.Profiles
{
    /// <summary>
    /// Contains extensions methods related to profiles.
    /// </summary>
    public static class ProfileExtensions
    {
        /// <summary>
        /// Gets the speed for the given profile on the link defined by the given attributes.
        /// </summary>
        public static Speed Speed(this Profile profile, IAttributeCollection attributes)
        {
            return profile.FactorAndSpeed(attributes).ToSpeed();
        }

        /// <summary>
        /// Converts a speed definition for the given factor and speed.
        /// </summary>
        public static Speed ToSpeed(this FactorAndSpeed factorAndSpeed)
        {
            if (factorAndSpeed.Direction >= 3)
            {
                return new Profiles.Speed()
                {
                    Direction = (short)(factorAndSpeed.Direction - 3),
                    Value = 1.0f / factorAndSpeed.SpeedFactor
                };
            }
            return new Profiles.Speed()
            {
                Direction = factorAndSpeed.Direction,
                Value = 1.0f / factorAndSpeed.SpeedFactor
            };
        }

        /// <summary>
        /// Gets a get factor function based on the given routerdb.
        /// </summary>
        public static Func<ushort, Factor> GetGetFactor(this Profile profile, RouterDb routerDb)
        {
            return (profileId) =>
            {
                var edgeProfile = routerDb.EdgeProfiles.Get(profileId);
                return profile.Factor(edgeProfile);
            };
        }

        /// <summary>
        /// Gets a get factor function based on the given routerdb.
        /// </summary>
        public static Func<ushort, FactorAndSpeed> GetGetFactorAndSpeed(this Profile profile, RouterDb routerDb)
        {
            return (profileId) =>
            {
                var edgeProfile = routerDb.EdgeProfiles.Get(profileId);
                return profile.FactorAndSpeed(edgeProfile);
            };
        }

        /// <summary>
        /// Gets the factor for the given profile on the link defined by the given attributes.
        /// </summary>
        public static Factor Factor(this Profile profile, IAttributeCollection attributes)
        {
            return profile.FactorAndSpeed(attributes).ToFactor();
        }

        /// <summary>
        /// Converts a factor definition for the given factor and speed.
        /// </summary>
        public static Factor ToFactor(this FactorAndSpeed factorAndSpeed)
        {
            if (factorAndSpeed.Direction >= 3)
            {
                return new Profiles.Factor()
                {
                    Direction = (short)(factorAndSpeed.Direction - 3),
                    Value = factorAndSpeed.Value
                };
            }
            return new Profiles.Factor()
            {
                Direction = factorAndSpeed.Direction,
                Value = factorAndSpeed.Value
            };
        }

        /// <summary>
        /// Converts a factor definition for the given factor and speed.
        /// </summary>
        public static bool CanStopOn(this FactorAndSpeed factorAndSpeed)
        {
            return factorAndSpeed.Direction < 3;
        }

        /// <summary>
        /// Returns true if the link defined by the given attributes can be stopped on.
        /// </summary>
        public static bool CanStopOn(this Profile profile, IAttributeCollection attributes)
        {
            return profile.FactorAndSpeed(attributes).Direction < 3;
        }
    }
}