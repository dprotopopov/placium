using System.Text;
using Tmds.Linux;

namespace AstraUtils.UnixSocket;

public static class SocketFilePermissionsHelper
{
    public static int SetSocketPermissions(string socketFilePath)
    {
        var filepath = Encoding.UTF8.GetBytes(socketFilePath);

        unsafe
        {
            fixed (byte* a = filepath)
            {
                return LibC.chmod(a, LibC.S_IWGRP | LibC.S_IRGRP | LibC.S_IRUSR | LibC.S_IWUSR);
            }
        }
    }
}