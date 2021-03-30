using System.IO;
using System.Threading.Tasks;
using Npgsql;

namespace Placium.Common
{
    public interface IUploadService
    {
        Task InstallAsync(Stream uploadStream, string session);
        Task UpdateAsync(Stream uploadStream, string session);
    }
}