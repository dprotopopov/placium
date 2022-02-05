using System.Threading.Tasks;

namespace Placium.Common;

public interface IUpdateService
{
    Task UpdateAsync(string session, bool full);
}