using System;
using Route.Attributes;

namespace Placium.Route.Profiles
{
    /// <summary>
    /// Contains extension methods 
    /// </summary>
    public static class IProfileInstanceExtensions
    {
        /// <summary>
        /// Checks the given edge values against the contraints in the profile.
        /// </summary>
        public static bool IsConstrained(this IProfileInstance profileInstance, float[] edgeValues)
        {
            if (profileInstance.Constraints == null)
            {
                return false;
            }

            if (profileInstance.Profile.ConstrainedVariables == null)
            {
                return false;
            }

            if (edgeValues == null)
            {
                return false;
            }

            for (var i = 0; i < profileInstance.Profile.ConstrainedVariables.Length && i < profileInstance.Constraints.Length; i++)
            {
                var constraint = profileInstance.Profile.ConstrainedVariables[i];
                if (constraint == null)
                {
                    continue;
                }
                var profileValue = profileInstance.Constraints[i];
                if (profileValue == constraint.DefaultValue)
                {
                    continue;
                }
                var edgeValue = edgeValues[i];
                if (edgeValue == constraint.DefaultValue)
                {
                    continue;
                }

                if (constraint.IsMax && profileValue > edgeValue)
                { // the constraint is a maximum and the profile value is larger than the edge value.
                    // the edge value for example maxweight 1.5T but vehicle weight is 2T.
                    return true;
                }
                else if (!constraint.IsMax && profileValue < edgeValue)
                { // the constraint is a minimum and the profile value is smaller than the edge value.
                    return true;
                }
            }

            return false;
        }

        /// <summary>
        /// Gets a the factor and speed for an edge with the given attributes.
        /// </summary>
        public static FactorAndSpeed FactorAndSpeed(this IProfileInstance profileInstance, IAttributeCollection attributes)
        {
            var factorAndSpeed = profileInstance.Profile.FactorAndSpeed(attributes);
            if (profileInstance.IsConstrained(factorAndSpeed.Constraints))
            {
                return Profiles.FactorAndSpeed.NoFactor;
            }
            return factorAndSpeed;
        }

  
        /// <summary>
        /// Gets a the factor for an edge with the given attributes.
        /// </summary>
        public static Factor Factor(this IProfileInstance profileInstance, IAttributeCollection attributes)
        {
            return profileInstance.FactorAndSpeed(attributes).ToFactor();
        }

     }
}