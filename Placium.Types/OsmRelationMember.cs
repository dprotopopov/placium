using NpgsqlTypes;

namespace Placium.Types
{
    public class OsmRelationMember
    {
        [PgName("id")]
        public long Id { get; set; }
        [PgName("role")]
        public string Role { get; set; }
        [PgName("type")]
        public int Type { get; set; }
    }
}