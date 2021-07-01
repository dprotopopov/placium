using NpgsqlTypes;

namespace Placium.Types
{
    public enum FiasServiceType3
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