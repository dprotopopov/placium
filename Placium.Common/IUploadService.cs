using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

namespace Placium.Common;

public interface IUploadService
{
    Task InstallAsync(Stream uploadStream, Dictionary<string, string> options, string session);
    Task UpdateAsync(Stream uploadStream, Dictionary<string, string> options, string session);
}