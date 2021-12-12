using Itinero;
using Itinero.IO.Osm;
using Itinero.LocalGeo;
using Itinero.Profiles;
using Placium.IO.Osm.PostgreSQL;

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
        public static void LoadOsmDataFromPostgreSQL(this RouterDb db, string connectionString, Box box,
            params Vehicle[] vehicles)
        {
            db.LoadOsmDataFromPostgreSQL(connectionString, box.ToPolygon(), vehicles);
        }

        /// <summary>
        ///     Loads a routing network from OSM data downloaded from PostgreSQL.
        /// </summary>
        public static void LoadOsmDataFromPostgreSQL(this RouterDb db, string connectionString, Polygon polygon,
            params Vehicle[] vehicles)
        {
            var stream = new PostgresSQLDataSource(connectionString, polygon);
            db.LoadOsmData(stream, vehicles);
        }
    }
}