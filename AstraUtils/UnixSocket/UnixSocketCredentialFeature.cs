using System.Net.Sockets;

namespace AstraUtils.UnixSocket
{
    public class UnixSocketCredentialFeature : IUnixSocketCredentialFeature
    {
        public UnixSocketCredentialFeature(Socket socket)
        {
            if (socket != null && socket.LocalEndPoint.AddressFamily == AddressFamily.Unix)
            {
                socket.GetPeerCred(out var cred);
                
                UserId = (int)(uint)cred.uid;
                ProcessId = cred.pid;
                GroupId = (int)(uint)cred.gid;

                IsAuthenticated = true;
            }
            



        }

        public bool IsAuthenticated { get; }
        public int UserId { get; }
        public int ProcessId { get; }
        public int GroupId { get; }
    }
}
