using NpgsqlTypes;

namespace Placium.Types
{
    public enum OsmType
    {
        [PgName("node")]
        Node,
        [PgName("way")]
        Way,
        [PgName("relation")]
        Relation,
    }
}