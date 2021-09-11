using System.Threading.Tasks;

namespace Placium.Common
{
    public interface IProgressClient
    {
        Task Progress(float progress, string id, string session);
        Task Init(string id, string session);
    }
}