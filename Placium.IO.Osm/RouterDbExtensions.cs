using Placium.IO.Osm.PostgreSQL;
using Route;
using Route.IO.Osm;
using Route.Profiles;

namespace Placium.IO.Osm
{
    /// <summary>
    ///     Contains extension methods for the router db.
    /// </summary>
    public static class RouterDbExtensions
    {
        /// <summary>
        ///     Loads a routing network from OSM data downloaded from PostgreSQL.
        /// </summary>
        public static void LoadOsmDataFromPostgreSQL(this RouterDb db, string connectionString,
            params Vehicle[] vehicles)
        {
            var stream = new PostgresSQLDataSource(connectionString);
            db.LoadOsmData(stream, vehicles);
        }
    }
}