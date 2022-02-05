using System.Net.Sockets;
using Tmds.Linux;

namespace AstraUtils;

public static class SocketExtensions
{
    private static int GetSocketHandler(this Socket socket)
    {
        return socket.Handle.ToInt32();
    }

    public static unsafe int SetRawSocketOptions<T>(this Socket socket, int level, int optname, in T options)
        where T : unmanaged
    {
        fixed (void* p = &options)
        {
            var res = LibC.setsockopt(socket.GetSocketHandler(), level, optname, p, sizeof(T));

            if (res != 0)
                PlatformException.Throw();


            return res;
        }
    }


    public static unsafe int GetRawSocketOptions<T>(this Socket socket, int level, int optname, out T options)
        where T : unmanaged
    {
        fixed (void* p = &options)
        {
            var len = (socklen_t)sizeof(T);

            var res = LibC.getsockopt(socket.GetSocketHandler(), level, optname, p, &len);

            if (res != 0)
                PlatformException.Throw();

            return res;
        }
    }

    public static void GetPeerCred(this Socket socket, out ucred options)
    {
        socket.GetRawSocketOptions(LibC.SOL_SOCKET, LibC.SO_PEERCRED, out options);
    }
}