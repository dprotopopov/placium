using System.Threading.Tasks;
using Npgsql;

namespace Placium.Common
{
    public interface IUpdateService
    {
        Task UpdateAsync(string session, bool full);
    }
}
