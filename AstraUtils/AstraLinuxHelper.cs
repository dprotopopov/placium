using Mono.Unix.Native;
using System;
using System.Linq;
using System.Text;
using Tmds.Linux;

namespace AstraUtils
{
    public static class AstraLinuxHelper
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

        public static DomainUserInfo GetDomainUserInfo(string username)
        {
            if (string.IsNullOrEmpty(username))
                throw new ArgumentNullException("unexpected null value", nameof(username));
            var res = Syscall.getpwnam(username);


            if (res == null)
                return null;

            return new DomainUserInfo()
            {
                Groups = Syscall.getgrouplist(username).Select(x => x.gr_name).ToArray(),
                UserId = res.pw_uid,
                Username = res.pw_name
            };
        }

        public static DomainUserInfo GetDomainUserInfo(uint id)
        {
            if (id <= 0)
                throw new ArgumentException("invalid value", nameof(id));
            var res = Syscall.getpwuid(id);


            if (res == null)
                return null;

            return new DomainUserInfo()
            {
                Groups = Syscall.getgrouplist(res.pw_name).Select(x => x.gr_name).ToArray(),
                UserId = res.pw_uid,
                Username = res.pw_name
            };
        }
    }

    public class DomainUserInfo
    {
        public uint UserId { get; set; }
        public string Username { get; set; }
        public string[] Groups { get; set; }
    }
}
