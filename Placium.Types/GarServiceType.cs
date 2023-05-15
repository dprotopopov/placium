using NpgsqlTypes;

namespace Placium.Types
{
    public enum GarServiceType
    {
        [PgName("addrob")] Addrob,
        [PgName("house")] House,
        [PgName("stead")] Stead,
        [PgName("room")] Room,
        [PgName("carplace")] Carplace
    }
}