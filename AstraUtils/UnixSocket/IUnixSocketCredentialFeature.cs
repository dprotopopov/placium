namespace AstraUtils.UnixSocket
{
    public interface IUnixSocketCredentialFeature
    {
        public bool IsAuthenticated { get; }
        public int UserId { get; }
        public int ProcessId { get; }
        public int GroupId { get; }
    }
}