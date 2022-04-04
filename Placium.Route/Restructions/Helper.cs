using OsmSharp;

namespace Placium.Route.Restructions
{
    /// <summary>
    ///     A collection of helper functions to process restrictions.
    /// </summary>
    public static class Helper
    {
        /// <summary>
        ///     Returns true if the given relation represents a restriction and
        /// </summary>
        public static bool IsRestriction(this Relation relation, out string vehicleType)
        {
            var type = string.Empty;
            var restriction = string.Empty;
            vehicleType = string.Empty;
            if (relation.Tags == null ||
                !relation.Tags.TryGetValue("type", out type) ||
                !relation.Tags.TryGetValue("restriction", out restriction))
                return false;

            if (!restriction.StartsWith("no_") && !restriction.StartsWith("only_")) return false;

            if (type != "restriction")
            {
                if (!type.StartsWith("restriction:")) return false;
                vehicleType = type.Substring("restriction:".Length, type.Length - "restriction:".Length);
            }

            return true;
        }
    }
}