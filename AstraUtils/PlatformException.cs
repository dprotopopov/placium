using System;
using System.Runtime.InteropServices;
using Tmds.Linux;

namespace AstraUtils
{
    class PlatformException : Exception
    {
        public PlatformException(int errno) :
            base(GetErrorMessage(errno))
        {
            HResult = errno;
        }

        public PlatformException() :
            this(LibC.errno)
        { }

        private static unsafe string GetErrorMessage(int errno)
        {
            int bufferLength = 1024;
            byte* buffer = stackalloc byte[bufferLength];

            int rv = LibC.strerror_r(errno, buffer, bufferLength);



            return rv == 0 ? Marshal.PtrToStringAnsi((IntPtr)buffer) : $"errno {errno}";
        }

        public static void Throw() => throw new PlatformException();
    }
}
