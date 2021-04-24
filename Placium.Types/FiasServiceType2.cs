using NpgsqlTypes;

namespace Placium.Types
{
    public enum FiasServiceType2
    {
        [PgName("addrob")]
        Addrob,
        [PgName("house")]
        House,
        [PgName("stead")]
        Stead,
        [PgName("room")]
        Room,
    }
}