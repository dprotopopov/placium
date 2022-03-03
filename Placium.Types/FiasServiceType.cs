using NpgsqlTypes;

namespace Placium.Types
{
    public enum FiasServiceType
    {
        [PgName("addrob")] Addrob,
        [PgName("house")] House,
        [PgName("stead")] Stead,
        [PgName("room")] Room
    }
}