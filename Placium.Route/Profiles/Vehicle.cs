using System;
using System.Collections.Generic;
using System.IO;
using Route.Attributes;

namespace Placium.Route.Profiles
{
    /// <summary>
    /// Represents a vehicle.
    /// </summary>
    public abstract class Vehicle
    {
        private readonly Dictionary<string, Profile> _profiles = new Dictionary<string, Profile>();
        
        /// <summary>
        /// Creates a new vehicle.
        /// </summary>
        public Vehicle()
        {
            
        }

        /// <summary>
        /// Gets the name of this vehicle.
        /// </summary>
        public abstract string Name
        {
            get;
        }

        /// <summary>
        /// Gets the normalization flag.
        /// </summary>
        public virtual bool Normalize { get; } = true;
        
        /// <summary>
        /// Gets the vehicle types.
        /// </summary>
        public virtual string[] VehicleTypes
        {
            get
            {
                return new string[] { };
            }
        }

        /// <summary>
        /// Gets a whitelist of attributes to keep as meta-data.
        /// </summary>
        public virtual HashSet<string> MetaWhiteList
        {
            get
            {
                return new HashSet<string>();
            }
        }

        /// <summary>
        /// Gets a whitelist of attributes to keep as part of the profile.
        /// </summary>
        public virtual HashSet<string> ProfileWhiteList
        {
            get
            {
                return new HashSet<string>();
            }
        }

        /// <summary>
        /// Adds a number of keys to the given whitelist when they are relevant for this vehicle.
        /// </summary>
        /// <returns>True if the edge with the given attributes is usefull for this vehicle.</returns>
        public virtual bool AddToWhiteList(IAttributeCollection attributes, Whitelist whitelist)
        {
            return this.FactorAndSpeed(attributes, whitelist).Value > 0;
        }

        /// <summary>
        /// Calculates a factor and speed and adds a keys to the given whitelist that are relevant.
        /// </summary>
        /// <returns>True if the edge with the given attributes is useful for this vehicle.</returns>
        public abstract FactorAndSpeed FactorAndSpeed(IAttributeCollection attributes, Whitelist whitelist);

        /// <summary>
        /// Returns true if the two given edges are equals as far as this vehicle is concerned.
        /// </summary>
        public virtual bool Equals(IAttributeCollection attributes1, IAttributeCollection attributes2)
        {
            return attributes1.ContainsSame(attributes2);
        }

        /// <summary>
        /// Registers a profile.
        /// </summary>
        public void Register(Profile profile)
        {
            _profiles[profile.Name.ToLowerInvariant()] = profile;
        }

        /// <summary>
        /// Returns the profile with the given name.
        /// </summary>
        public Profile Profile(string name)
        {
            return _profiles[name.ToLowerInvariant()];
        }

        /// <summary>
        /// Returns the profiles for this vehicle.
        /// </summary>
        /// <returns></returns>
        public IEnumerable<Profile> GetProfiles()
        {
            return _profiles.Values;
        }

        /// <summary>
        /// Gets the profile to calculate shortest routes.
        /// </summary>
        public virtual Profile Shortest()
        {
            return this.Profile("shortest");
        }

        /// <summary>
        /// Gets the profile to calculate fastest routes.
        /// </summary>
        public virtual Profile Fastest()
        {
            return this.Profile(string.Empty);
        }

        /// <summary>
        /// Registers this vehicle.
        /// </summary>
        [Obsolete]
        public virtual void Register()
        {
            Vehicle.Register(this);

            foreach(var profile in _profiles)
            {
                Profiles.Profile.Register(profile.Value);
            }
        }

        private static Dictionary<string, Vehicle> _vehicles = new Dictionary<string, Vehicle>();

        /// <summary>
        /// Registers a vehicle.
        /// </summary>
        [Obsolete]
        public static void Register(Vehicle vehicle)
        {
            _vehicles[vehicle.Name.ToLowerInvariant()] = vehicle;
        }

        /// <summary>
        /// Gets a registered vehicle.
        /// </summary>
        [Obsolete]
        public static Vehicle Get(string name)
        {
            return _vehicles[name.ToLowerInvariant()];
        }

        /// <summary>
        /// Tries to get a registred vehicle.
        /// </summary>
        [Obsolete]
        public static bool TryGet(string name, out Vehicle value)
        {
            return _vehicles.TryGetValue(name.ToLowerInvariant(), out value);
        }

        /// <summary>
        /// Gets all registered vehicles.
        /// </summary>
        /// <returns></returns>
        [Obsolete]
        public static IEnumerable<Vehicle> GetRegistered()
        {
            return _vehicles.Values;
        }

        /// <summary>
        /// Gets parameters 
        /// </summary>
        public virtual IReadonlyAttributeCollection Parameters
        {
            get
            {
                return new AttributeCollection();
            }
        }

        /// <summary>
        /// Gets or sets a custom vehicle deserializer.
        /// </summary>
        public static Func<string, Stream, Vehicle> CustomDeserializer
        {
            get;
            set;
        }

        /// <summary>
        /// Gets a description of this vehicle.
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            return this.Name;
        }
    }
}