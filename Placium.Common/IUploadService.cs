using System.IO;
using System.Threading.Tasks;
using Npgsql;

namespace Placium.Common
{
    public interface IUploadService
    {
        Task InstallAsync(Stream uploadStream, NpgsqlConnection connection, string session);
        Task UpdateAsync(Stream uploadStream, NpgsqlConnection connection, string session);
    }
}