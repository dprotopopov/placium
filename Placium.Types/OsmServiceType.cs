using NpgsqlTypes;

namespace Placium.Types
{
    public enum OsmServiceType
    {
        [PgName("node")]
        Node,
        [PgName("way")]
        Way,
        [PgName("relation")]
        Relation,
        [PgName("placex")]
        Placex,
        [PgName("addr")]
        Addr,
    }
}