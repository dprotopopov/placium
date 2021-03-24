using NpgsqlTypes;

namespace Placium.Types
{
    public enum ServiceType
    {
        [PgName("node")]
        Node,
        [PgName("way")]
        Way,
        [PgName("relation")]
        Relation,
        [PgName("place")]
        Place,
    }
}